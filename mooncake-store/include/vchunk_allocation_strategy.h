#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "vchunk_config.h"
#include "vchunk_metadata.h"

namespace mooncake {

class AllocatorManager;

struct VChunkSegmentProfile {
    std::string segment_name;
    std::string segment_instance_id;
    std::string server_id;
    std::string rack_id;
    std::string zone_id;
    ReplicaType storage_type{ReplicaType::MEMORY};
    std::string transport_protocol;
    uint64_t available_bytes{0};
    uint64_t largest_free_extent{0};
    double bandwidth_ewma_mbps{0};
    double latency_ewma_us{0};
    double load_ratio{0};
    int64_t metrics_updated_at_ms{0};
    uint32_t telemetry_samples{0};
    bool healthy{true};
    bool supports_exact_claim{false};
};

ErrorCode UpdateVChunkSegmentTelemetry(VChunkSegmentProfile& profile,
                                       double bandwidth_mbps,
                                       double latency_us, double load_ratio,
                                       int64_t now_ms, double ewma_alpha);
double ScoreVChunkSegmentProfile(const VChunkSegmentProfile& profile,
                                 int64_t now_ms, uint64_t metrics_ttl_ms,
                                 uint32_t min_samples);
ErrorCode ReportVChunkSegmentTelemetry(const std::string& segment_name,
                                       double bandwidth_mbps,
                                       double latency_us, double load_ratio,
                                       int64_t now_ms, double ewma_alpha);
void ClearVChunkSegmentTelemetryForTesting();

std::vector<VChunkSegmentProfile> BuildVChunkSegmentProfiles(
    const AllocatorManager& allocator_manager);

struct VCSliceAllocation {
    uint32_t slice_index{0};
    std::string segment_name;
    std::string segment_instance_id;
    uint64_t target_offset{0};
    uint32_t logical_length{0};
    uint32_t allocated_length{0};
    uint8_t replica_index{0};
    std::unique_ptr<AllocatedBuffer> buffer;

    VCSliceAllocation() = default;
    VCSliceAllocation(VCSliceAllocation&&) noexcept = default;
    VCSliceAllocation& operator=(VCSliceAllocation&&) noexcept = default;
    VCSliceAllocation(const VCSliceAllocation&) = delete;
    VCSliceAllocation& operator=(const VCSliceAllocation&) = delete;
};

class VChunkAllocationResult {
   public:
    VChunkAllocationResult() = default;
    VChunkAllocationResult(VChunkAllocationResult&&) noexcept = default;
    VChunkAllocationResult& operator=(VChunkAllocationResult&&) noexcept =
        default;
    VChunkAllocationResult(const VChunkAllocationResult&) = delete;
    VChunkAllocationResult& operator=(const VChunkAllocationResult&) = delete;

    size_t row_size{0};
    uint8_t replica_num{1};
    std::vector<VCSliceAllocation> allocations;
    std::vector<SliceGroup> slice_groups;
};

tl::expected<VChunkAllocationResult, ErrorCode> AllocateVChunk(
    const AllocatorManager& allocator_manager, uint64_t total_size,
    VCSliceSizeLevel slice_size_level,
    const std::set<std::string>& excluded_segments = {},
    uint8_t replica_num = 1,
    const VChunkConfig& config = VChunkConfig{});

}  // namespace mooncake
