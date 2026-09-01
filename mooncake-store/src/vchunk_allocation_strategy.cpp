#include "vchunk_allocation_strategy.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "allocation_strategy.h"
#include "random.h"

namespace mooncake {
namespace {

std::mutex telemetry_mutex;
std::unordered_map<std::string, VChunkSegmentProfile> telemetry_profiles;

tl::expected<VChunkAllocationResult, ErrorCode> RetryWithoutSegmentLimits(
    const AllocatorManager& allocator_manager, uint64_t total_size,
    VCSliceSizeLevel slice_size_level,
    const std::set<std::string>& excluded_segments, uint8_t replica_num,
    const VChunkConfig& config) {
    auto fallback = config;
    fallback.max_segments_per_replica = 0;
    fallback.max_segments_per_vchunk = 0;
    fallback.allow_segment_limit_fallback = false;
    return AllocateVChunk(allocator_manager, total_size, slice_size_level,
                          excluded_segments, replica_num, fallback);
}

struct Candidate {
    std::string name;
    const std::vector<std::shared_ptr<BufferAllocatorBase>>* allocators;
    VChunkSegmentProfile profile;
    uint64_t remaining_slices;
    uint64_t allocated_slices{0};
};

uint64_t AvailableBytes(const BufferAllocatorBase& allocator) {
    const auto capacity = allocator.capacity();
    const auto used = allocator.size();
    if (capacity == kAllocatorUnknownFreeSpace) {
        return allocator.getLargestFreeRegion();
    }
    return capacity > used ? capacity - used : 0;
}

std::string ServerFromEndpoint(const std::string& endpoint) {
    const auto scheme = endpoint.find("://");
    const auto begin = scheme == std::string::npos ? 0 : scheme + 3;
    if (begin < endpoint.size() && endpoint[begin] == '[') {
        const auto end = endpoint.find(']', begin + 1);
        return end == std::string::npos
                   ? std::string()
                   : endpoint.substr(begin + 1, end - begin - 1);
    }
    const auto end = endpoint.find(':', begin);
    return endpoint.substr(begin, end - begin);
}

std::unique_ptr<AllocatedBuffer> AllocateFromCandidate(Candidate& candidate,
                                                       size_t size,
                                                       size_t slice_count = 1) {
    for (const auto& allocator : *candidate.allocators) {
        if (allocator && allocator->getLargestFreeRegion() >= size) {
            if (auto buffer = allocator->allocate(size)) {
                candidate.allocated_slices += slice_count;
                return buffer;
            }
        }
    }
    return nullptr;
}

}  // namespace

std::vector<VChunkSegmentProfile> BuildVChunkSegmentProfiles(
    const AllocatorManager& allocator_manager) {
    std::vector<VChunkSegmentProfile> profiles;
    profiles.reserve(allocator_manager.getNames().size());
    for (const auto& name : allocator_manager.getNames()) {
        const auto* allocators = allocator_manager.getAllocators(name);
        if (allocators == nullptr || allocators->empty()) continue;
        VChunkSegmentProfile profile;
        profile.segment_name = name;
        profile.healthy = false;
        bool first = true;
        for (const auto& allocator : *allocators) {
            if (!allocator || !allocator->capabilities().allocate) continue;
            profile.healthy = true;
            const auto capabilities = allocator->capabilities();
            const auto available = AvailableBytes(*allocator);
            profile.available_bytes =
                available > std::numeric_limits<uint64_t>::max() -
                                profile.available_bytes
                    ? std::numeric_limits<uint64_t>::max()
                    : profile.available_bytes + available;
            profile.largest_free_extent = std::max<uint64_t>(
                profile.largest_free_extent,
                allocator->getLargestFreeRegion());
            if (first) {
                profile.segment_instance_id =
                    allocator->getSegmentInstanceId();
                profile.storage_type = capabilities.storage_type;
                const auto endpoint = allocator->getTransportEndpoint();
                profile.server_id = ServerFromEndpoint(endpoint);
                const auto scheme = endpoint.find("://");
                profile.transport_protocol =
                    scheme == std::string::npos
                        ? std::string()
                        : endpoint.substr(0, scheme);
                profile.supports_exact_claim =
                    allocator->supportsExactClaim();
                first = false;
            } else {
                profile.supports_exact_claim =
                    profile.supports_exact_claim &&
                    allocator->supportsExactClaim();
            }
        }
        if (profile.healthy && profile.available_bytes > 0) {
            std::lock_guard<std::mutex> guard(telemetry_mutex);
            if (const auto it = telemetry_profiles.find(name);
                it != telemetry_profiles.end()) {
                profile.bandwidth_ewma_mbps = it->second.bandwidth_ewma_mbps;
                profile.latency_ewma_us = it->second.latency_ewma_us;
                profile.load_ratio = it->second.load_ratio;
                profile.metrics_updated_at_ms =
                    it->second.metrics_updated_at_ms;
                profile.telemetry_samples = it->second.telemetry_samples;
            }
            profiles.push_back(std::move(profile));
        }
    }
    return profiles;
}

ErrorCode UpdateVChunkSegmentTelemetry(VChunkSegmentProfile& profile,
                                       double bandwidth_mbps,
                                       double latency_us, double load_ratio,
                                       int64_t now_ms, double ewma_alpha) {
    if (!std::isfinite(bandwidth_mbps) || !std::isfinite(latency_us) ||
        !std::isfinite(load_ratio) || bandwidth_mbps < 0 || latency_us < 0 ||
        load_ratio < 0 || load_ratio > 1 || now_ms < 0 || ewma_alpha <= 0 ||
        ewma_alpha > 1) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto blend = [ewma_alpha](double previous, double sample,
                                    uint32_t samples) {
        return samples == 0
                   ? sample
                   : ewma_alpha * sample + (1.0 - ewma_alpha) * previous;
    };
    profile.bandwidth_ewma_mbps = blend(
        profile.bandwidth_ewma_mbps, bandwidth_mbps,
        profile.telemetry_samples);
    profile.latency_ewma_us =
        blend(profile.latency_ewma_us, latency_us, profile.telemetry_samples);
    profile.load_ratio =
        blend(profile.load_ratio, load_ratio, profile.telemetry_samples);
    ++profile.telemetry_samples;
    profile.metrics_updated_at_ms = now_ms;
    return ErrorCode::OK;
}

double ScoreVChunkSegmentProfile(const VChunkSegmentProfile& profile,
                                 int64_t now_ms, uint64_t metrics_ttl_ms,
                                 uint32_t min_samples) {
    if (!profile.healthy) return -std::numeric_limits<double>::infinity();
    const double capacity_score =
        profile.available_bytes == 0
            ? 0
            : static_cast<double>(profile.largest_free_extent) /
                  static_cast<double>(profile.available_bytes);
    const bool fresh = now_ms >= profile.metrics_updated_at_ms &&
                       static_cast<uint64_t>(now_ms -
                                             profile.metrics_updated_at_ms) <=
                           metrics_ttl_ms &&
                       profile.telemetry_samples >= min_samples;
    if (!fresh) return capacity_score;
    const double bandwidth_score = profile.bandwidth_ewma_mbps /
                                   (profile.bandwidth_ewma_mbps + 1000.0);
    const double latency_penalty =
        profile.latency_ewma_us / (profile.latency_ewma_us + 1000.0);
    return capacity_score + bandwidth_score - latency_penalty -
           profile.load_ratio;
}

ErrorCode ReportVChunkSegmentTelemetry(const std::string& segment_name,
                                       double bandwidth_mbps,
                                       double latency_us, double load_ratio,
                                       int64_t now_ms, double ewma_alpha) {
    if (segment_name.empty()) return ErrorCode::INVALID_PARAMS;
    std::lock_guard<std::mutex> guard(telemetry_mutex);
    auto& profile = telemetry_profiles[segment_name];
    profile.segment_name = segment_name;
    profile.healthy = true;
    return UpdateVChunkSegmentTelemetry(profile, bandwidth_mbps, latency_us,
                                        load_ratio, now_ms, ewma_alpha);
}

void ClearVChunkSegmentTelemetryForTesting() {
    std::lock_guard<std::mutex> guard(telemetry_mutex);
    telemetry_profiles.clear();
}

tl::expected<VChunkAllocationResult, ErrorCode> AllocateVChunk(
    const AllocatorManager& allocator_manager, uint64_t total_size,
    VCSliceSizeLevel slice_size_level,
    const std::set<std::string>& excluded_segments, uint8_t replica_num,
    const VChunkConfig& config) {
    const uint64_t slice_size = SliceSizeLevelToBytes(slice_size_level);
    if (total_size == 0 || slice_size == 0 || replica_num == 0 ||
        total_size > std::numeric_limits<uint64_t>::max() - (slice_size - 1)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t slice_count_u64 =
        (total_size + slice_size - 1) / slice_size;
    if (slice_count_u64 > std::numeric_limits<uint32_t>::max()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<Candidate> candidates;
    for (auto& profile : BuildVChunkSegmentProfiles(allocator_manager)) {
        if (excluded_segments.contains(profile.segment_name) ||
            !profile.healthy || profile.largest_free_extent < slice_size) {
            continue;
        }
        const auto* allocators =
            allocator_manager.getAllocators(profile.segment_name);
        const uint64_t weight = profile.available_bytes / slice_size;
        if (weight > 0) {
            candidates.push_back({profile.segment_name, allocators,
                                  std::move(profile), weight, 0});
        }
    }
    if (candidates.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    if (replica_num > candidates.size()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    const auto placement_now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::sort(candidates.begin(), candidates.end(),
              [&](const Candidate& lhs, const Candidate& rhs) {
                  const auto lhs_score =
                      ScoreVChunkSegmentProfile(
                          lhs.profile, placement_now_ms,
                          config.placement_metrics_ttl_ms,
                          config.placement_min_samples);
                  const auto rhs_score =
                      ScoreVChunkSegmentProfile(
                          rhs.profile, placement_now_ms,
                          config.placement_metrics_ttl_ms,
                          config.placement_min_samples);
                  if (lhs_score != rhs_score) return lhs_score > rhs_score;
                  if (lhs.remaining_slices != rhs.remaining_slices) {
                      return lhs.remaining_slices > rhs.remaining_slices;
                  }
                  return lhs.name < rhs.name;
              });
    if (config.max_segments_per_vchunk != 0 &&
        candidates.size() > config.max_segments_per_vchunk) {
        candidates.resize(config.max_segments_per_vchunk);
    }
    if (replica_num > candidates.size() ||
        config.min_segments_per_replica > candidates.size()) {
        if (config.allow_segment_limit_fallback &&
            config.max_segments_per_vchunk != 0) {
            return RetryWithoutSegmentLimits(
                allocator_manager, total_size, slice_size_level,
                excluded_segments, replica_num, config);
        }
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    const auto slice_count = static_cast<uint32_t>(slice_count_u64);
    VChunkAllocationResult result;
    result.replica_num = replica_num;
    const size_t segments_per_replica =
        config.max_segments_per_replica == 0
            ? candidates.size()
            : std::min<size_t>(candidates.size(),
                               config.max_segments_per_replica);
    if (segments_per_replica < config.min_segments_per_replica) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    result.row_size = std::min<size_t>(slice_count, segments_per_replica);
    result.allocations.reserve(static_cast<size_t>(slice_count) * replica_num);
    const size_t start_offset = randomIndex(candidates.size());
    std::vector<std::unordered_set<std::string>> slice_domains(slice_count);
    const size_t target_stripe_slices = std::clamp<size_t>(
        (slice_count + segments_per_replica - 1) / segments_per_replica,
        config.min_stripe_slices, config.max_stripe_slices);

    for (uint8_t replica = 0; replica < replica_num; ++replica) {
        std::vector<size_t> allowed_candidates;
        allowed_candidates.reserve(segments_per_replica);
        for (size_t i = 0; i < segments_per_replica; ++i) {
            allowed_candidates.push_back(
                (start_offset +
                 static_cast<size_t>(replica) * segments_per_replica + i) %
                candidates.size());
        }
        for (uint32_t slice_index = 0; slice_index < slice_count;) {
            const size_t stripe_slices = std::min<size_t>(
                target_stripe_slices, slice_count - slice_index);
            bool allocated = false;
            for (size_t attempt = 0; attempt < segments_per_replica;
                 ++attempt) {
                const size_t preferred =
                    (slice_index / target_stripe_slices) %
                    segments_per_replica;
                const size_t index = allowed_candidates[
                    (preferred + attempt) % segments_per_replica];
                auto& candidate = candidates[index];
                const auto& failure_domain =
                    candidate.profile.server_id.empty()
                        ? candidate.name
                        : candidate.profile.server_id;
                bool domain_conflict = false;
                for (size_t i = 0; i < stripe_slices; ++i) {
                    domain_conflict =
                        domain_conflict ||
                        slice_domains[slice_index + i].contains(
                            failure_domain);
                }
                if (domain_conflict ||
                    stripe_slices > candidate.remaining_slices -
                                        std::min(candidate.allocated_slices,
                                                 candidate.remaining_slices)) {
                    continue;
                }
                const size_t extent_size = stripe_slices * slice_size;
                auto buffer = AllocateFromCandidate(candidate, extent_size,
                                                    stripe_slices);
                if (!buffer) continue;
                const auto extent_base =
                    reinterpret_cast<uintptr_t>(buffer->data());
                const auto instance_id = buffer->getSegmentInstanceId();
                for (size_t i = 0; i < stripe_slices; ++i) {
                    const uint32_t current_slice =
                        slice_index + static_cast<uint32_t>(i);
                    const uint64_t consumed = current_slice * slice_size;
                    VCSliceAllocation allocation;
                    allocation.slice_index = current_slice;
                    allocation.segment_name = candidate.name;
                    allocation.segment_instance_id = instance_id;
                    allocation.target_offset = extent_base + i * slice_size;
                    allocation.logical_length = static_cast<uint32_t>(
                        std::min<uint64_t>(slice_size,
                                           total_size - consumed));
                    allocation.allocated_length =
                        static_cast<uint32_t>(slice_size);
                    allocation.replica_index = replica;
                    if (i == 0) allocation.buffer = std::move(buffer);
                    result.allocations.push_back(std::move(allocation));
                    slice_domains[current_slice].insert(failure_domain);
                }
                slice_index += static_cast<uint32_t>(stripe_slices);
                allocated = true;
                break;
            }
            if (!allocated) {
                if (config.allow_segment_limit_fallback &&
                    (config.max_segments_per_replica != 0 ||
                     config.max_segments_per_vchunk != 0)) {
                    result.allocations.clear();
                    return RetryWithoutSegmentLimits(
                        allocator_manager, total_size, slice_size_level,
                        excluded_segments, replica_num, config);
                }
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        }
    }

    SliceGroup current;
    for (const auto& allocation : result.allocations) {
        const bool contiguous = !current.slice_indices.empty() &&
                                current.replica_index ==
                                    allocation.replica_index &&
                                current.segment_name == allocation.segment_name &&
                                current.slice_indices.back() + 1 ==
                                    allocation.slice_index;
        if (!contiguous && !current.slice_indices.empty()) {
            result.slice_groups.push_back(std::move(current));
            current = SliceGroup{};
        }
        if (current.slice_indices.empty()) {
            current.group_id =
                static_cast<uint32_t>(result.slice_groups.size());
            current.segment_name = allocation.segment_name;
            current.replica_index = allocation.replica_index;
        }
        current.slice_indices.push_back(allocation.slice_index);
    }
    if (!current.slice_indices.empty()) {
        result.slice_groups.push_back(std::move(current));
    }
    return result;
}

}  // namespace mooncake
