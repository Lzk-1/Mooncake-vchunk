#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mooncake::vsegment {

enum class Lifecycle : uint8_t { PREPARING, ACTIVE, DRAINING, DEGRADED };

struct PSegmentExtent {
    std::string segment_id;
    uint64_t base_offset{0};
    uint64_t length{0};

    bool operator==(const PSegmentExtent& other) const {
        return segment_id == other.segment_id &&
               base_offset == other.base_offset && length == other.length;
    }
};
YLT_REFL(PSegmentExtent, segment_id, base_offset, length);

struct VSegmentProfile {
    std::string name;
    uint32_t member_count{0};
    uint64_t stripe_size{0};
    uint64_t member_extent_size{0};
};
YLT_REFL(VSegmentProfile, name, member_count, stripe_size,
         member_extent_size);

struct PartitionPSegmentQuota {
    std::string segment_id;
    uint64_t base_offset{0};
    uint64_t length{0};
};
YLT_REFL(PartitionPSegmentQuota, segment_id, base_offset, length);

struct PartitionVSegmentConfig {
    std::string partition_id;
    uint64_t config_generation{0};
    std::vector<PartitionPSegmentQuota> quotas;
};
YLT_REFL(PartitionVSegmentConfig, partition_id, config_generation, quotas);

struct VSegmentView {
    std::string vsegment_id;
    std::string partition_id;
    std::string mapping_algorithm{"round_robin_stripe"};
    uint64_t stripe_size{0};
    uint64_t logical_capacity{0};
    Lifecycle lifecycle{Lifecycle::PREPARING};
    std::vector<PSegmentExtent> members;
    uint32_t checksum{0};
};
YLT_REFL(VSegmentView, vsegment_id, partition_id, mapping_algorithm,
         stripe_size, logical_capacity, lifecycle, members, checksum);

struct VSegmentAllocationResult {
    ErrorCode error{ErrorCode::OK};
    VSegmentView view;
    std::string detail;

    explicit operator bool() const { return error == ErrorCode::OK; }
};

// Checks immutable layout rules. The configured member count is a hard
// requirement: validation and allocation never silently reduce it.
ErrorCode ValidateProfile(const VSegmentProfile& profile,
                          std::string* detail = nullptr);
ErrorCode ValidatePartitionConfig(const PartitionVSegmentConfig& config,
                                  std::string* detail = nullptr);
ErrorCode ValidateView(const VSegmentView& view,
                       const VSegmentProfile& profile,
                       std::string* detail = nullptr);

uint32_t ComputeViewChecksum(const VSegmentView& view);

// Owns only the physical ranges statically assigned to one Partition. Calls
// are serialized so concurrent SubMaster requests cannot allocate the same
// range twice.
class PartitionQuotaAllocator {
   public:
    explicit PartitionQuotaAllocator(PartitionVSegmentConfig config);

    VSegmentAllocationResult Allocate(const std::string& vsegment_id,
                                      const VSegmentProfile& profile);
    ErrorCode Restore(const VSegmentView& view,
                      const VSegmentProfile& profile,
                      std::string* detail = nullptr);
    ErrorCode Release(const VSegmentView& view);

    uint64_t FreeBytes(const std::string& segment_id) const;
    const PartitionVSegmentConfig& config() const { return config_; }

   private:
    struct Range {
        uint64_t offset;
        uint64_t length;
    };

    static void InsertAndMerge(std::vector<Range>& ranges, Range range);

    PartitionVSegmentConfig config_;
    mutable std::mutex mutex_;
    std::map<std::string, std::vector<Range>> free_ranges_;
    std::unordered_map<std::string, VSegmentView> allocations_;
};

}  // namespace mooncake::vsegment
