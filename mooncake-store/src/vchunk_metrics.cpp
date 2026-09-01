#include "vchunk_metrics.h"

#include <unordered_set>

namespace mooncake {

void VChunkMetrics::Observe(VChunkOperation operation, bool success,
                            uint64_t latency_us) {
    const auto index = static_cast<size_t>(operation);
    ++requests_[index];
    if (success) {
        ++successes_[index];
    }
    latency_us_[index].fetch_add(latency_us);
}

void VChunkMetrics::SetStateCount(VChunkStatus state, uint64_t count) {
    states_[static_cast<size_t>(state)].store(count);
}

void VChunkMetrics::ObserveLayout(const VChunkMetadataRecord& record) {
    std::unordered_set<std::string> segments;
    for (const auto& slice : record.slices) {
        segments.insert(slice.target_segment_name);
    }
    segment_participations_.fetch_add(segments.size());
    slice_groups_.fetch_add(record.slice_groups.size());
    for (const auto& group : record.slice_groups) {
        const auto group_size =
            static_cast<uint64_t>(group.slice_indices.size());
        auto observed = largest_slice_group_.load();
        while (observed < group_size &&
               !largest_slice_group_.compare_exchange_weak(
                   observed, group_size)) {
        }
    }
    size_t bucket = 0;
    switch (record.slice_size_level) {
        case VCSliceSizeLevel::k4K:
            bucket = 0;
            break;
        case VCSliceSizeLevel::k64K:
            bucket = 1;
            break;
        case VCSliceSizeLevel::k256K:
            bucket = 2;
            break;
        case VCSliceSizeLevel::k1M:
            bucket = 3;
            break;
    }
    slice_size_distribution_[bucket].fetch_add(record.slice_count);
}

VChunkMetricsSnapshot VChunkMetrics::Snapshot() const {
    VChunkMetricsSnapshot result;
    for (size_t i = 0; i < requests_.size(); ++i) {
        result.requests[i] = requests_[i].load();
        result.successes[i] = successes_[i].load();
        result.latency_us[i] = latency_us_[i].load();
    }
    result.slices = slices_.load();
    result.segment_participations = segment_participations_.load();
    result.slice_groups = slice_groups_.load();
    result.largest_slice_group = largest_slice_group_.load();
    result.allocation_failures = allocation_failures_.load();
    result.transfer_failures = transfer_failures_.load();
    result.timeouts = timeouts_.load();
    result.retries = retries_.load();
    result.rollbacks = rollbacks_.load();
    result.metadata_bytes = metadata_bytes_.load();
    result.allocated_bytes = allocated_bytes_.load();
    result.scrub_runs = scrub_runs_.load();
    result.scrub_issues = scrub_issues_.load();
    result.scrub_failures = scrub_failures_.load();
    result.cleanup_attempts = cleanup_attempts_.load();
    result.cleanup_failures = cleanup_failures_.load();
    result.pending_cleanup = pending_cleanup_.load();
    for (size_t i = 0; i < slice_size_distribution_.size(); ++i) {
        result.slice_size_distribution[i] =
            slice_size_distribution_[i].load();
    }
    for (size_t i = 0; i < states_.size(); ++i) {
        result.states[i] = states_[i].load();
    }
    return result;
}

}  // namespace mooncake
