#include "vsegment/vsegment_service.h"

namespace mooncake::vsegment {

std::shared_ptr<VSegmentManager> VSegmentService::FindPartition(
    const std::string& partition_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = partitions_.find(partition_id);
    return found == partitions_.end() ? nullptr : found->second;
}

ErrorCode VSegmentService::AddPartition(
    const std::string& partition_id, uint64_t route_epoch,
    std::shared_ptr<VSegmentStateCommitter> committer,
    const PartitionVSegmentSnapshot* recovered, std::string* detail) {
    if (partition_id.empty() || route_epoch == 0 || !committer)
        return ErrorCode::INVALID_PARAMS;
    PartitionVSegmentConfig default_config;
    auto result = BuildPartitionConfig(quota_snapshot_, partition_id,
                                       quota_snapshot_.default_profile,
                                       &default_config, detail);
    if (result != ErrorCode::OK) return result;
    auto manager = std::make_shared<VSegmentManager>(
        quota_snapshot_, partition_id, std::move(committer));
    result = manager->SetRouteEpoch(route_epoch);
    if (result != ErrorCode::OK) return result;
    if (recovered) {
        if (recovered->route_epoch > route_epoch) return ErrorCode::STALE_ROUTE;
        auto state = *recovered;
        state.route_epoch = route_epoch;
        result = manager->Restore(state, detail);
        if (result != ErrorCode::OK) return result;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (partitions_.count(partition_id))
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    partitions_.emplace(partition_id, std::move(manager));
    return ErrorCode::OK;
}

ErrorCode VSegmentService::RemovePartition(const std::string& partition_id,
                                           uint64_t route_epoch) {
    std::shared_ptr<VSegmentManager> removed;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = partitions_.find(partition_id);
        if (found == partitions_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        if (found->second->SetRouteEpoch(route_epoch) != ErrorCode::OK)
            return ErrorCode::STALE_ROUTE;
        removed = std::move(found->second);
        partitions_.erase(found);
    }
    // Destruction outside the registry lock waits for any asynchronous
    // creation without blocking unrelated Partitions.
    return ErrorCode::OK;
}

ErrorCode VSegmentService::SnapshotPartition(
    const std::string& partition_id, PartitionVSegmentSnapshot* snapshot) {
    if (!snapshot) return ErrorCode::INVALID_PARAMS;
    auto manager = FindPartition(partition_id);
    if (!manager) return ErrorCode::STALE_ROUTE;
    *snapshot = manager->Snapshot();
    return ErrorCode::OK;
}

ErrorCode VSegmentService::ReconcilePartitionRoute(
    const partition::PartitionRoute& route,
    const std::string& local_submaster_id,
    std::shared_ptr<VSegmentStateCommitter> committer,
    const PartitionVSegmentSnapshot* recovered, std::string* detail) {
    if (route.partition_id.empty() || local_submaster_id.empty() ||
        route.route_epoch == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (route.state !=
            static_cast<int32_t>(partition::PartitionState::kActive) &&
        route.state !=
            static_cast<int32_t>(partition::PartitionState::kMigrating)) {
        if (detail) *detail = "unknown PartitionRoute state";
        return ErrorCode::INVALID_PARAMS;
    }
    const bool owned_here =
        route.owner_submaster_id == local_submaster_id;
    auto current = FindPartition(route.partition_id);
    if (!owned_here) {
        if (!current) return ErrorCode::OK;
        return RemovePartition(route.partition_id, route.route_epoch);
    }

    // During MIGRATING the old owner remains the sole writer. The target does
    // not install a live manager until the route atomically switches owner.
    if (current) return current->SetRouteEpoch(route.route_epoch);
    return AddPartition(route.partition_id, route.route_epoch,
                        std::move(committer), recovered, detail);
}

std::vector<PartitionVSegmentSnapshot>
VSegmentService::SnapshotAllPartitions() {
    std::vector<std::shared_ptr<VSegmentManager>> managers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        managers.reserve(partitions_.size());
        for (const auto& [partition_id, manager] : partitions_) {
            (void)partition_id;
            managers.push_back(manager);
        }
    }

    std::vector<PartitionVSegmentSnapshot> snapshots;
    snapshots.reserve(managers.size());
    for (const auto& manager : managers) snapshots.push_back(manager->Snapshot());
    return snapshots;
}

VSegmentPutStartResult VSegmentService::StartPut(
    const std::string& partition_id, uint64_t route_epoch,
    const std::string& operation_id, uint64_t length,
    const std::string& profile_name) {
    auto manager = FindPartition(partition_id);
    if (!manager)
        return {ErrorCode::STALE_ROUTE, operation_id, {},
                "Partition is not owned by this SubMaster"};
    return manager->StartPut(operation_id, length, profile_name, route_epoch);
}

ErrorCode VSegmentService::CommitPut(const VSegmentDescriptor& replica,
                                     uint64_t route_epoch,
                                     const std::string& operation_id,
                                     const std::string& object_id) {
    auto manager = FindPartition(replica.partition_id);
    if (!manager) return ErrorCode::STALE_ROUTE;
    LogicalRange committed;
    const LogicalRange expected{replica.logical_offset, replica.length};
    auto result = manager->CommitPut(replica.vsegment_id, operation_id,
                                     object_id, &committed, route_epoch,
                                     &expected);
    if (result != ErrorCode::OK) return result;
    return ErrorCode::OK;
}

ErrorCode VSegmentService::AbortPut(const std::string& partition_id,
                                    const std::string& vsegment_id,
                                    uint64_t route_epoch,
                                    const std::string& operation_id) {
    auto manager = FindPartition(partition_id);
    return manager ? manager->AbortPut(vsegment_id, operation_id, route_epoch)
                   : ErrorCode::STALE_ROUTE;
}

ErrorCode VSegmentService::LoadView(const std::string& partition_id,
                                    const std::string& vsegment_id,
                                    VSegmentView* view, std::string* detail) {
    auto manager = FindPartition(partition_id);
    if (!manager) {
        if (detail) *detail = "Partition is not owned by this SubMaster";
        return ErrorCode::STALE_ROUTE;
    }
    if (!manager->FindView(vsegment_id, view)) {
        if (detail) *detail = "vsegment view is not active";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::vsegment
