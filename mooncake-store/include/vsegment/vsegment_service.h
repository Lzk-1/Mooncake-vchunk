#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment_manager.h"
#include "vsegment/vsegment_transfer.h"
#include "partition/vsegment_types.h"

namespace mooncake::vsegment {

// SubMaster-local registry. One instance owns only the Partitions currently
// routed to that SubMaster; Partition migration removes/adds the whole manager.
class VSegmentService final : public VSegmentViewProvider {
   public:
    explicit VSegmentService(PartitionPhysicalQuotaSnapshot quota_snapshot)
        : quota_snapshot_(std::move(quota_snapshot)) {}

    ErrorCode AddPartition(
        const std::string& partition_id, uint64_t route_epoch,
        std::shared_ptr<VSegmentStateCommitter> committer,
        const PartitionVSegmentSnapshot* recovered = nullptr,
        std::string* detail = nullptr);
    ErrorCode RemovePartition(const std::string& partition_id,
                              uint64_t route_epoch);
    ErrorCode ReconcilePartitionRoute(
        const partition::PartitionRoute& route,
        const std::string& local_submaster_id,
        std::shared_ptr<VSegmentStateCommitter> committer,
        const PartitionVSegmentSnapshot* recovered = nullptr,
        std::string* detail = nullptr);
    ErrorCode SnapshotPartition(const std::string& partition_id,
                                PartitionVSegmentSnapshot* snapshot);
    std::vector<PartitionVSegmentSnapshot> SnapshotAllPartitions();

    VSegmentPutStartResult StartPut(const std::string& partition_id,
                                    uint64_t route_epoch,
                                    const std::string& operation_id,
                                    uint64_t length,
                                    const std::string& profile_name = {});
    ErrorCode CommitPut(const VSegmentDescriptor& replica,
                        uint64_t route_epoch,
                        const std::string& operation_id,
                        const std::string& object_id);
    ErrorCode AbortPut(const std::string& partition_id,
                       const std::string& vsegment_id,
                       uint64_t route_epoch,
                       const std::string& operation_id);

    ErrorCode LoadView(const std::string& partition_id,
                       const std::string& vsegment_id, VSegmentView* view,
                       std::string* detail = nullptr) override;

   private:
    std::shared_ptr<VSegmentManager> FindPartition(
        const std::string& partition_id);

    PartitionPhysicalQuotaSnapshot quota_snapshot_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<VSegmentManager>>
        partitions_;
};

}  // namespace mooncake::vsegment
