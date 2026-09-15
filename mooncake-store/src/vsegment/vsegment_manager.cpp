#include "vsegment/vsegment_manager.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <set>

namespace mooncake::vsegment {
namespace {

VSegmentAllocationResult ManagerError(ErrorCode error, std::string detail) {
    VSegmentAllocationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

ReservationResult ReservationError(ErrorCode error, std::string detail) {
    ReservationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

std::vector<PartitionVSegmentConfig> ConfigsForPartition(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::string& partition_id) {
    std::vector<PartitionVSegmentConfig> configs;
    for (const auto& profile : snapshot.profile_specs) {
        PartitionVSegmentConfig config;
        if (BuildPartitionConfig(snapshot, partition_id, profile.name,
                                 &config) == ErrorCode::OK)
            configs.push_back(std::move(config));
    }
    return configs;
}

}  // namespace

VSegmentManager::VSegmentManager(PartitionVSegmentConfig config,
                                 std::vector<VSegmentProfile> profiles,
                                 std::shared_ptr<VSegmentStateCommitter> committer)
    : partition_id_(config.partition_id),
      config_generation_(config.config_generation),
      quota_configs_{std::move(config)},
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)) {
    for (auto& profile : profiles) {
        profiles_.emplace(profile.name, std::move(profile));
    }
}

VSegmentManager::VSegmentManager(
    const PartitionPhysicalQuotaSnapshot& quota_snapshot,
    std::string partition_id,
    std::shared_ptr<VSegmentStateCommitter> committer)
    : partition_id_(std::move(partition_id)),
      config_generation_(quota_snapshot.config_generation),
      quota_configs_(ConfigsForPartition(quota_snapshot, partition_id_)),
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)) {
    for (const auto& profile : quota_snapshot.profile_specs)
        profiles_.emplace(profile.name, profile);
}

VSegmentAllocationResult VSegmentManager::Create(
    const std::string& profile_name) {
    const auto creation_key = partition_id_ + "/" + profile_name;
    auto result = profile_creation_coordinator_.GetOrCreate(
        creation_key,
        [&] { return CreateSingleFlight(profile_name); });
    profile_creation_coordinator_.Forget(creation_key);
    return result;
}

VSegmentAllocationResult VSegmentManager::RequestCreate(
    const std::string& profile_name, uint64_t retry_after_ms) {
    if (!profiles_.count(profile_name))
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    const auto key = partition_id_ + "/" + profile_name;
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto pending = pending_creations_.find(key);
    if (pending != pending_creations_.end()) {
        if (pending->second.wait_for(std::chrono::milliseconds(0)) ==
            std::future_status::ready) {
            auto result = pending->second.get();
            pending_creations_.erase(pending);
            return result;
        }
    } else {
        pending_creations_.emplace(
            key, std::async(std::launch::async,
                            [this, profile_name] { return Create(profile_name); })
                     .share());
    }
    return ManagerError(ErrorCode::VSEGMENT_CREATING,
                        "vsegment creation is in progress; retry_after_ms=" +
                            std::to_string(retry_after_ms));
}

VSegmentAllocationResult VSegmentManager::CreateSingleFlight(
    const std::string& profile_name) {
    auto profile = profiles_.find(profile_name);
    if (profile == profiles_.end()) {
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [id, pending] : vsegments_) {
        if (pending.profile_name != profile_name ||
            pending.lifecycle != Lifecycle::PREPARING)
            continue;
        pending.lifecycle = Lifecycle::ACTIVE;
        std::string detail;
        auto persisted = PersistLocked("vsegment_create_commit", &detail);
        if (persisted != ErrorCode::OK) {
            pending.lifecycle = Lifecycle::PREPARING;
            return ManagerError(persisted, std::move(detail));
        }
        return {ErrorCode::OK, pending.view, {}};
    }
    const std::string vsegment_id = UuidToString(generate_uuid());
    auto logical = std::make_unique<LogicalRangeAllocator>(
        profile->second.member_extent_size * profile->second.member_count);
    auto allocation = physical_allocator_.Allocate(vsegment_id, profile->second);
    if (!allocation) return allocation;
    try {
        vsegments_.emplace(
            vsegment_id,
            ManagedVSegment{profile_name, Lifecycle::PREPARING,
                            allocation.view, std::move(logical)});
    } catch (...) {
        vsegments_.erase(vsegment_id);
        physical_allocator_.Release(allocation.view);
        throw;
    }
    std::string detail;
    auto persisted = PersistLocked("vsegment_create_begin", &detail);
    if (persisted != ErrorCode::OK) {
        vsegments_.erase(vsegment_id);
        physical_allocator_.Release(allocation.view);
        return ManagerError(persisted, std::move(detail));
    }
    vsegments_.at(vsegment_id).lifecycle = Lifecycle::ACTIVE;
    persisted = PersistLocked("vsegment_create_commit", &detail);
    if (persisted != ErrorCode::OK) {
        vsegments_.at(vsegment_id).lifecycle = Lifecycle::PREPARING;
        return ManagerError(persisted, std::move(detail));
    }
    return allocation;
}

ErrorCode VSegmentManager::SetRouteEpoch(uint64_t route_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (route_epoch == 0 || route_epoch < route_epoch_)
        return ErrorCode::STALE_ROUTE;
    route_epoch_ = route_epoch;
    return ErrorCode::OK;
}

ReservationResult VSegmentManager::ReservePut(
    const std::string& vsegment_id, const std::string& operation_id,
    uint64_t length) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end() ||
        managed->second.lifecycle != Lifecycle::ACTIVE)
        return ReservationError(ErrorCode::SEGMENT_NOT_FOUND,
                                "vsegment not found or not active");
    const auto before = managed->second.logical_allocator->Snapshot();
    auto result = managed->second.logical_allocator->Reserve(operation_id,
                                                              length);
    if (!result) return result;
    std::string detail;
    const auto persisted = PersistLocked("logical_reserve", &detail);
    if (persisted != ErrorCode::OK) {
        managed->second.logical_allocator->Restore(before);
        return ReservationError(persisted, std::move(detail));
    }
    return result;
}

ErrorCode VSegmentManager::CommitPut(const std::string& vsegment_id,
                                     const std::string& operation_id,
                                     const std::string& allocation_id,
                                     LogicalRange* range) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    const auto before = managed->second.logical_allocator->Snapshot();
    const auto result = managed->second.logical_allocator->Commit(
        operation_id, allocation_id, range);
    if (result != ErrorCode::OK) return result;
    const auto persisted = PersistLocked("logical_commit");
    if (persisted != ErrorCode::OK)
        managed->second.logical_allocator->Restore(before);
    return persisted;
}

ErrorCode VSegmentManager::AbortPut(const std::string& vsegment_id,
                                    const std::string& operation_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    const auto before = managed->second.logical_allocator->Snapshot();
    const auto result = managed->second.logical_allocator->Abort(operation_id);
    if (result != ErrorCode::OK) return result;
    const auto persisted = PersistLocked("logical_abort");
    if (persisted != ErrorCode::OK)
        managed->second.logical_allocator->Restore(before);
    return persisted;
}

ErrorCode VSegmentManager::ReleaseObject(const std::string& vsegment_id,
                                         const std::string& allocation_id,
                                         LogicalRange range) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    const auto before = managed->second.logical_allocator->Snapshot();
    const auto result = managed->second.logical_allocator->Release(
        allocation_id, range);
    if (result != ErrorCode::OK) return result;
    const auto persisted = PersistLocked("logical_release");
    if (persisted != ErrorCode::OK)
        managed->second.logical_allocator->Restore(before);
    return persisted;
}

ErrorCode VSegmentManager::TransitionLifecycle(
    const std::string& vsegment_id, Lifecycle target) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    const auto current = managed->second.lifecycle;
    if (current == target) return ErrorCode::OK;
    const bool valid =
        (current == Lifecycle::PREPARING && target == Lifecycle::ACTIVE) ||
        (current == Lifecycle::ACTIVE && target == Lifecycle::DRAINING) ||
        (current == Lifecycle::DRAINING && target == Lifecycle::RETIRED);
    if (!valid) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    if (target == Lifecycle::RETIRED &&
        !managed->second.logical_allocator->Empty())
        return ErrorCode::OBJECT_REPLICA_BUSY;
    managed->second.lifecycle = target;
    const auto persisted = PersistLocked("lifecycle_transition");
    if (persisted != ErrorCode::OK) managed->second.lifecycle = current;
    if (persisted == ErrorCode::OK && target == Lifecycle::RETIRED) {
        auto released = physical_allocator_.Release(managed->second.view);
        if (released != ErrorCode::OK) return released;
        persisted = PersistLocked("vsegment_extent_release");
        if (persisted != ErrorCode::OK) {
            std::string ignored;
            physical_allocator_.Restore(managed->second.view,
                                         profiles_.at(managed->second.profile_name),
                                         &ignored);
        }
    }
    return persisted;
}

PartitionVSegmentSnapshot VSegmentManager::SnapshotLocked() const {
    PartitionVSegmentSnapshot snapshot;
    snapshot.partition_id = partition_id_;
    snapshot.config_generation = config_generation_;
    snapshot.route_epoch = route_epoch_;
    snapshot.metadata_revision = metadata_revision_;
    for (const auto& [id, managed] : vsegments_) {
        snapshot.vsegments.push_back(
            {managed.profile_name, managed.lifecycle, managed.view,
             managed.logical_allocator->Snapshot()});
    }
    std::sort(snapshot.vsegments.begin(), snapshot.vsegments.end(),
              [](const auto& left, const auto& right) {
                  return left.view.vsegment_id < right.view.vsegment_id;
              });
    return snapshot;
}

PartitionVSegmentSnapshot VSegmentManager::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return SnapshotLocked();
}

ErrorCode VSegmentManager::PersistLocked(const std::string& mutation,
                                         std::string* detail) {
    if (!committer_) {
        if (detail) *detail = "vsegment state committer is not configured";
        return ErrorCode::PERSISTENT_FAIL;
    }
    ++metadata_revision_;
    auto result = committer_->Commit(SnapshotLocked(), mutation, detail);
    if (result != ErrorCode::OK) --metadata_revision_;
    return result;
}

ErrorCode VSegmentManager::Restore(
    const PartitionVSegmentSnapshot& snapshot, std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (quota_configs_.empty()) {
        if (detail) *detail = "Partition has no profile quota";
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    }
    for (const auto& config : quota_configs_) {
        auto validation = ValidatePartitionConfig(config, detail);
        if (validation != ErrorCode::OK) return validation;
    }
    if (!vsegments_.empty()) {
        if (detail) *detail = "restore requires an empty manager";
        return ErrorCode::INVALID_PARAMS;
    }
    if (snapshot.partition_id != partition_id_ ||
        snapshot.config_generation != config_generation_) {
        if (detail) *detail = "snapshot Partition or generation mismatch";
        return ErrorCode::INVALID_VERSION;
    }
    PartitionQuotaAllocator verifier(quota_configs_);
    std::vector<std::unique_ptr<LogicalRangeAllocator>> logical_allocators;
    std::vector<VSegmentProfile> matching_profiles;
    std::vector<bool> restore_view;
    for (const auto& state : snapshot.vsegments) {
        if (state.lifecycle != Lifecycle::PREPARING &&
            state.lifecycle != Lifecycle::ACTIVE &&
            state.lifecycle != Lifecycle::DRAINING &&
            state.lifecycle != Lifecycle::RETIRED) {
            if (detail) *detail = "invalid vsegment lifecycle";
            return ErrorCode::INVALID_PARAMS;
        }
        auto profile = profiles_.find(state.profile_name);
        if (profile == profiles_.end()) {
            if (detail) *detail = "snapshot view has no matching profile";
            return ErrorCode::INVALID_PARAMS;
        }
        if (state.lifecycle == Lifecycle::PREPARING ||
            state.lifecycle == Lifecycle::RETIRED) {
            // CREATE_COMMIT was not durably observed. Its statically owned
            // extents are safe to roll back locally during recovery.
            logical_allocators.push_back(nullptr);
            matching_profiles.push_back(profile->second);
            restore_view.push_back(false);
            continue;
        }
        auto validation = verifier.Restore(state.view, profile->second, detail);
        if (validation != ErrorCode::OK) return validation;
        auto logical = std::make_unique<LogicalRangeAllocator>(
            state.view.logical_capacity);
        validation = logical->Restore(state.logical_allocation, detail);
        if (validation != ErrorCode::OK) return validation;
        logical_allocators.push_back(std::move(logical));
        matching_profiles.push_back(profile->second);
        restore_view.push_back(true);
    }

    for (size_t index = 0; index < snapshot.vsegments.size(); ++index) {
        const auto& state = snapshot.vsegments[index];
        if (!restore_view[index]) continue;
        auto restored = physical_allocator_.Restore(
            state.view, matching_profiles[index], detail);
        if (restored != ErrorCode::OK) return restored;
        vsegments_.emplace(
            state.view.vsegment_id,
            ManagedVSegment{state.profile_name, state.lifecycle, state.view,
                            std::move(logical_allocators[index])});
    }
    route_epoch_ = snapshot.route_epoch;
    metadata_revision_ = snapshot.metadata_revision;
    return ErrorCode::OK;
}

bool VSegmentManager::FindView(const std::string& vsegment_id,
                               VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto vsegment = vsegments_.find(vsegment_id);
    if (vsegment == vsegments_.end()) return false;
    if (view) *view = vsegment->second.view;
    return true;
}

}  // namespace mooncake::vsegment
