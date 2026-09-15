#pragma once

#include <memory>
#include <future>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

struct VSegmentStateSnapshot {
    std::string profile_name;
    Lifecycle lifecycle{Lifecycle::PREPARING};
    VSegmentView view;
    LogicalAllocationSnapshot logical_allocation;
};
YLT_REFL(VSegmentStateSnapshot, profile_name, lifecycle, view,
         logical_allocation);

struct PartitionVSegmentSnapshot {
    std::string partition_id;
    uint64_t config_generation{0};
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::vector<VSegmentStateSnapshot> vsegments;
};
YLT_REFL(PartitionVSegmentSnapshot, partition_id, config_generation,
         route_epoch, metadata_revision, vsegments);

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
    VSegmentManager(const PartitionPhysicalQuotaSnapshot& quota_snapshot,
                    std::string partition_id,
                    std::shared_ptr<VSegmentStateCommitter> committer);

    VSegmentAllocationResult Create(const std::string& profile_name);
    // Hot-path API: starts at most one background creation per
    // partition/profile and asks callers to retry while it is running.
    VSegmentAllocationResult RequestCreate(const std::string& profile_name,
                                           uint64_t retry_after_ms = 50);
    ErrorCode SetRouteEpoch(uint64_t route_epoch);
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
        Lifecycle lifecycle{Lifecycle::PREPARING};
        VSegmentView view;
        std::unique_ptr<LogicalRangeAllocator> logical_allocator;
    };

    VSegmentAllocationResult CreateSingleFlight(const std::string& profile_name);
    PartitionVSegmentSnapshot SnapshotLocked() const;
    ErrorCode PersistLocked(const std::string& mutation,
                            std::string* detail = nullptr);
    std::string partition_id_;
    uint64_t config_generation_{0};
    std::unordered_map<std::string, VSegmentProfile> profiles_;
    std::vector<PartitionVSegmentConfig> quota_configs_;
    PartitionQuotaAllocator physical_allocator_;
    CreationCoordinator profile_creation_coordinator_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ManagedVSegment> vsegments_;
    uint64_t route_epoch_{0};
    uint64_t metadata_revision_{0};
    std::shared_ptr<VSegmentStateCommitter> committer_;
    std::mutex pending_mutex_;
    std::unordered_map<std::string,
                       std::shared_future<VSegmentAllocationResult>>
        pending_creations_;
};

}  // namespace mooncake::vsegment
