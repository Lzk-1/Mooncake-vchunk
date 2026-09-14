#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

struct VSegmentStateSnapshot {
    std::string profile_name;
    VSegmentView view;
    LogicalAllocationSnapshot logical_allocation;
};
YLT_REFL(VSegmentStateSnapshot, profile_name, view, logical_allocation);

struct PartitionVSegmentSnapshot {
    std::string partition_id;
    uint64_t config_generation{0};
    std::vector<VSegmentStateSnapshot> vsegments;
};
YLT_REFL(PartitionVSegmentSnapshot, partition_id, config_generation,
         vsegments);

// Partition-local facade intended to be owned by the Partition's SubMaster.
// Persistence transport is deliberately outside this class: HA Snapshot/OpLog
// code serializes the returned state and calls Restore after failover.
class VSegmentManager {
   public:
    VSegmentManager(PartitionVSegmentConfig config,
                    std::vector<VSegmentProfile> profiles);

    VSegmentAllocationResult GetOrCreate(const std::string& profile_name,
                                         uint64_t creation_slot);
    ReservationResult ReservePut(const std::string& vsegment_id,
                                 const std::string& operation_id,
                                 uint64_t length);
    ErrorCode CommitPut(const std::string& vsegment_id,
                        const std::string& operation_id,
                        LogicalRange* range = nullptr);
    ErrorCode AbortPut(const std::string& vsegment_id,
                       const std::string& operation_id);
    ErrorCode ReleaseObject(const std::string& vsegment_id,
                            LogicalRange range);

    PartitionVSegmentSnapshot Snapshot() const;
    ErrorCode Restore(const PartitionVSegmentSnapshot& snapshot,
                      std::string* detail = nullptr);
    bool FindView(const std::string& vsegment_id, VSegmentView* view) const;

   private:
    struct ManagedVSegment {
        std::string profile_name;
        VSegmentView view;
        std::unique_ptr<LogicalRangeAllocator> logical_allocator;
    };

    static std::string CreationKey(const std::string& partition_id,
                                   const std::string& profile_name,
                                   uint64_t creation_slot);
    LogicalRangeAllocator* FindAllocator(const std::string& vsegment_id);

    PartitionVSegmentConfig config_;
    std::unordered_map<std::string, VSegmentProfile> profiles_;
    PartitionQuotaAllocator physical_allocator_;
    CreationCoordinator creation_coordinator_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ManagedVSegment> vsegments_;
};

}  // namespace mooncake::vsegment
