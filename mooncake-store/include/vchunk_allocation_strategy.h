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
