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
    std::string creation_key;
    Lifecycle lifecycle{Lifecycle::PREPARING};
    VSegmentView view;
    LogicalAllocationSnapshot logical_allocation;
};
YLT_REFL(VSegmentStateSnapshot, profile_name, creation_key, lifecycle, view,
         logical_allocation);

struct PartitionVSegmentSnapshot {
    std::string partition_id;
    uint64_t config_generation{0};
    std::vector<VSegmentStateSnapshot> vsegments;
    std::unordered_map<std::string, uint64_t> next_creation_slots;
};
YLT_REFL(PartitionVSegmentSnapshot, partition_id, config_generation,
         vsegments, next_creation_slots);

class VSegmentStateCommitter {
   public:
    virtual ~VSegmentStateCommitter() = default;
    virtual ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                             const std::string& mutation,
                             std::string* detail) = 0;
};

// Partition-local facade intended to be owned by the Partition's SubMaster.
// Persistence transport is deliberately outside this class: HA Snapshot/OpLog
// code serializes the returned state and calls Restore after failover.
class VSegmentManager {
   public:
    VSegmentManager(PartitionVSegmentConfig config,
                    std::vector<VSegmentProfile> profiles,
                    std::shared_ptr<VSegmentStateCommitter> committer);

    VSegmentAllocationResult Create(const std::string& profile_name);
    ReservationResult ReservePut(const std::string& vsegment_id,
                                 const std::string& operation_id,
                                 uint64_t length);
    ErrorCode CommitPut(const std::string& vsegment_id,
                        const std::string& operation_id,
                        const std::string& allocation_id,
                        LogicalRange* range = nullptr);
    ErrorCode AbortPut(const std::string& vsegment_id,
                       const std::string& operation_id);
    ErrorCode ReleaseObject(const std::string& vsegment_id,
                            const std::string& allocation_id,
                            LogicalRange range);
    ErrorCode TransitionLifecycle(const std::string& vsegment_id,
                                  Lifecycle target);

    PartitionVSegmentSnapshot Snapshot() const;
    ErrorCode Restore(const PartitionVSegmentSnapshot& snapshot,
                      std::string* detail = nullptr);
    bool FindView(const std::string& vsegment_id, VSegmentView* view) const;

   private:
    struct ManagedVSegment {
        std::string profile_name;
        std::string creation_key;
        Lifecycle lifecycle{Lifecycle::PREPARING};
        VSegmentView view;
        std::unique_ptr<LogicalRangeAllocator> logical_allocator;
    };

    static std::string CreationKey(const std::string& partition_id,
                                   const std::string& profile_name,
                                   uint64_t creation_slot);
    VSegmentAllocationResult GetOrCreate(const std::string& profile_name,
                                         uint64_t creation_slot);
    PartitionVSegmentSnapshot SnapshotLocked() const;
    ErrorCode PersistLocked(const std::string& mutation,
                            std::string* detail = nullptr);
    PartitionVSegmentConfig config_;
    std::unordered_map<std::string, VSegmentProfile> profiles_;
    PartitionQuotaAllocator physical_allocator_;
    CreationCoordinator creation_coordinator_;
    CreationCoordinator profile_creation_coordinator_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ManagedVSegment> vsegments_;
    std::unordered_map<std::string, std::string> creation_results_;
    std::unordered_map<std::string, uint64_t> next_creation_slots_;
    std::shared_ptr<VSegmentStateCommitter> committer_;
};

}  // namespace mooncake::vsegment
