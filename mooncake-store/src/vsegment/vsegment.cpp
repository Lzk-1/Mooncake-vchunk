#include "vsegment/vsegment.h"

#include <algorithm>
#include <limits>
#include <set>
#include <sstream>

#include "crc32c.h"

namespace mooncake::vsegment {
namespace {

bool AddOverflows(uint64_t left, uint64_t right) {
    return right > std::numeric_limits<uint64_t>::max() - left;
}

ErrorCode Invalid(std::string message, std::string* detail) {
    if (detail) *detail = std::move(message);
    return ErrorCode::INVALID_PARAMS;
}

void AppendUint64(std::string& output, uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        output.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

void AppendString(std::string& output, const std::string& value) {
    AppendUint64(output, value.size());
    output.append(value);
}

}  // namespace

ErrorCode ValidateProfile(const VSegmentProfile& profile, std::string* detail) {
    if (profile.name.empty()) return Invalid("profile name is empty", detail);
    if (profile.member_count == 0)
        return Invalid("member_count must be positive", detail);
    if (profile.stripe_size == 0)
        return Invalid("stripe_size must be positive", detail);
    if (profile.member_extent_size == 0)
        return Invalid("member_extent_size must be positive", detail);
    if (profile.member_extent_size % profile.stripe_size != 0) {
        return Invalid("member_extent_size must be a multiple of stripe_size",
                       detail);
    }
    if (profile.member_extent_size >
        std::numeric_limits<uint64_t>::max() / profile.member_count) {
        return Invalid("profile logical_capacity overflows", detail);
    }
    return ErrorCode::OK;
}

ErrorCode ValidatePartitionConfig(const PartitionVSegmentConfig& config,
                                  std::string* detail) {
    if (config.partition_id.empty())
        return Invalid("partition_id is empty", detail);
    if (config.config_generation == 0)
        return Invalid("config_generation must be positive", detail);
    if (config.quotas.empty()) return Invalid("quotas are empty", detail);

    std::map<std::string, std::vector<PartitionPSegmentQuota>> by_segment;
    for (const auto& quota : config.quotas) {
        if (quota.segment_id.empty())
            return Invalid("quota segment_id is empty", detail);
        if (quota.length == 0) return Invalid("quota length is zero", detail);
        if (AddOverflows(quota.base_offset, quota.length))
            return Invalid("quota range overflows", detail);
        by_segment[quota.segment_id].push_back(quota);
    }
    for (auto& [segment, quotas] : by_segment) {
        std::sort(quotas.begin(), quotas.end(), [](const auto& a, const auto& b) {
            return a.base_offset < b.base_offset;
        });
        for (size_t index = 1; index < quotas.size(); ++index) {
            const auto previous_end =
                quotas[index - 1].base_offset + quotas[index - 1].length;
            if (previous_end > quotas[index].base_offset) {
                return Invalid("overlapping quota ranges for " + segment,
                               detail);
            }
        }
    }
    return ErrorCode::OK;
}

uint32_t ComputeViewChecksum(const VSegmentView& view) {
    std::string bytes;
    AppendString(bytes, view.vsegment_id);
    AppendString(bytes, view.partition_id);
    AppendString(bytes, view.mapping_algorithm);
    AppendUint64(bytes, view.stripe_size);
    AppendUint64(bytes, view.logical_capacity);
    AppendUint64(bytes, static_cast<uint8_t>(view.lifecycle));
    AppendUint64(bytes, view.members.size());
    for (const auto& member : view.members) {
        AppendString(bytes, member.segment_id);
        AppendUint64(bytes, member.base_offset);
        AppendUint64(bytes, member.length);
    }
    return Crc32cValue(bytes.data(), bytes.size());
}

ErrorCode ValidateView(const VSegmentView& view,
                       const VSegmentProfile& profile, std::string* detail) {
    auto result = ValidateProfile(profile, detail);
    if (result != ErrorCode::OK) return result;
    if (view.vsegment_id.empty() || view.partition_id.empty())
        return Invalid("view identity is empty", detail);
    if (view.mapping_algorithm != "round_robin_stripe")
        return Invalid("unsupported mapping_algorithm", detail);
    if (view.stripe_size != profile.stripe_size)
        return Invalid("view stripe_size does not match profile", detail);
    if (view.members.size() != profile.member_count)
        return Invalid("view member count does not match profile", detail);
    if (profile.member_extent_size >
        std::numeric_limits<uint64_t>::max() / profile.member_count) {
        return Invalid("profile logical_capacity overflows", detail);
    }
    const auto expected_capacity =
        profile.member_extent_size * profile.member_count;
    if (view.logical_capacity != expected_capacity) {
        return Invalid("view logical_capacity does not match profile", detail);
    }
    std::set<std::string> member_ids;
    for (const auto& member : view.members) {
        if (member.segment_id.empty() ||
            member.length != profile.member_extent_size ||
            AddOverflows(member.base_offset, member.length)) {
            return Invalid("invalid member extent", detail);
        }
        if (!member_ids.insert(member.segment_id).second)
            return Invalid("duplicate member segment", detail);
    }
    if (view.checksum != ComputeViewChecksum(view))
        return ErrorCode::CHECKSUM_MISMATCH;
    return ErrorCode::OK;
}

PartitionQuotaAllocator::PartitionQuotaAllocator(
    PartitionVSegmentConfig config)
    : config_(std::move(config)) {
    for (const auto& quota : config_.quotas) {
        InsertAndMerge(free_ranges_[quota.segment_id],
                       {quota.base_offset, quota.length});
    }
}

VSegmentAllocationResult PartitionQuotaAllocator::Allocate(
    const std::string& vsegment_id, const VSegmentProfile& profile) {
    std::lock_guard<std::mutex> lock(mutex_);
    VSegmentAllocationResult result;
    std::string detail;
    if (ValidatePartitionConfig(config_, &detail) != ErrorCode::OK ||
        ValidateProfile(profile, &detail) != ErrorCode::OK ||
        vsegment_id.empty()) {
        result.error = ErrorCode::INVALID_PARAMS;
        result.detail = vsegment_id.empty() ? "vsegment_id is empty" : detail;
        return result;
    }
    if (allocations_.count(vsegment_id)) {
        result.error = ErrorCode::SEGMENT_ALREADY_EXISTS;
        result.detail = "vsegment already exists";
        return result;
    }

    struct Candidate {
        std::string segment;
        size_t range_index;
        uint64_t offset;
    };
    std::vector<Candidate> candidates;
    for (const auto& [segment, ranges] : free_ranges_) {
        auto range = std::find_if(ranges.begin(), ranges.end(), [&](const auto& r) {
            return r.length >= profile.member_extent_size;
        });
        if (range != ranges.end()) {
            candidates.push_back({segment,
                                  static_cast<size_t>(range - ranges.begin()),
                                  range->offset});
        }
        if (candidates.size() == profile.member_count) break;
    }
    if (candidates.size() != profile.member_count) {
        result.error = ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
        std::ostringstream message;
        message << "partition " << config_.partition_id << " requires "
                << profile.member_count << " psegments with "
                << profile.member_extent_size << " free bytes each, but only "
                << candidates.size() << " are available";
        result.detail = message.str();
        return result;
    }

    VSegmentView view;
    view.vsegment_id = vsegment_id;
    view.partition_id = config_.partition_id;
    view.stripe_size = profile.stripe_size;
    view.logical_capacity = profile.member_extent_size * profile.member_count;
    view.lifecycle = Lifecycle::ACTIVE;
    for (const auto& candidate : candidates) {
        auto& ranges = free_ranges_.at(candidate.segment);
        auto& range = ranges[candidate.range_index];
        view.members.push_back(
            {candidate.segment, range.offset, profile.member_extent_size});
        range.offset += profile.member_extent_size;
        range.length -= profile.member_extent_size;
        if (range.length == 0) ranges.erase(ranges.begin() + candidate.range_index);
    }
    view.checksum = ComputeViewChecksum(view);
    allocations_.emplace(vsegment_id, view);
    result.view = std::move(view);
    return result;
}

ErrorCode PartitionQuotaAllocator::Restore(const VSegmentView& view,
                                           const VSegmentProfile& profile,
                                           std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto validation = ValidateView(view, profile, detail);
    if (validation != ErrorCode::OK) return validation;
    if (view.partition_id != config_.partition_id)
        return Invalid("view belongs to another partition", detail);
    if (allocations_.count(view.vsegment_id))
        return Invalid("duplicate vsegment in restore state", detail);

    struct Match {
        std::string segment;
        size_t range_index;
        PSegmentExtent extent;
    };
    std::vector<Match> matches;
    for (const auto& member : view.members) {
        auto segment = free_ranges_.find(member.segment_id);
        if (segment == free_ranges_.end())
            return Invalid("member is outside Partition quota", detail);
        auto range = std::find_if(
            segment->second.begin(), segment->second.end(), [&](const auto& r) {
                return member.base_offset >= r.offset &&
                       member.base_offset + member.length <= r.offset + r.length;
            });
        if (range == segment->second.end())
            return Invalid("member overlaps another restored view", detail);
        matches.push_back({member.segment_id,
                           static_cast<size_t>(range - segment->second.begin()),
                           member});
    }

    for (const auto& match : matches) {
        auto& ranges = free_ranges_.at(match.segment);
        const auto original = ranges[match.range_index];
        const auto original_end = original.offset + original.length;
        const auto member_end =
            match.extent.base_offset + match.extent.length;
        ranges.erase(ranges.begin() + match.range_index);
        if (original.offset < match.extent.base_offset) {
            ranges.push_back(
                {original.offset, match.extent.base_offset - original.offset});
        }
        if (member_end < original_end) {
            ranges.push_back({member_end, original_end - member_end});
        }
        std::sort(ranges.begin(), ranges.end(),
                  [](const auto& a, const auto& b) {
                      return a.offset < b.offset;
                  });
    }
    allocations_.emplace(view.vsegment_id, view);
    return ErrorCode::OK;
}

void PartitionQuotaAllocator::InsertAndMerge(std::vector<Range>& ranges,
                                             Range range) {
    ranges.push_back(range);
    std::sort(ranges.begin(), ranges.end(),
              [](const auto& a, const auto& b) { return a.offset < b.offset; });
    std::vector<Range> merged;
    for (const auto& current : ranges) {
        if (!merged.empty() &&
            merged.back().offset + merged.back().length == current.offset) {
            merged.back().length += current.length;
        } else {
            merged.push_back(current);
        }
    }
    ranges.swap(merged);
}

ErrorCode PartitionQuotaAllocator::Release(const VSegmentView& view) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto allocation = allocations_.find(view.vsegment_id);
    if (allocation == allocations_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    if (allocation->second.checksum != view.checksum)
        return ErrorCode::INVALID_VERSION;
    for (const auto& member : allocation->second.members) {
        InsertAndMerge(free_ranges_[member.segment_id],
                       {member.base_offset, member.length});
    }
    allocations_.erase(allocation);
    return ErrorCode::OK;
}

uint64_t PartitionQuotaAllocator::FreeBytes(
    const std::string& segment_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto ranges = free_ranges_.find(segment_id);
    if (ranges == free_ranges_.end()) return 0;
    uint64_t total = 0;
    for (const auto& range : ranges->second) total += range.length;
    return total;
}

}  // namespace mooncake::vsegment
