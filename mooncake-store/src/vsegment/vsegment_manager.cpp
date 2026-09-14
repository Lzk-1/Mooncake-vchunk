#include "vsegment/vsegment_manager.h"

#include <algorithm>
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

}  // namespace

VSegmentManager::VSegmentManager(PartitionVSegmentConfig config,
                                 std::vector<VSegmentProfile> profiles,
                                 std::shared_ptr<VSegmentStateCommitter> committer)
    : config_(std::move(config)),
      physical_allocator_(config_),
      committer_(std::move(committer)) {
    for (auto& profile : profiles) {
        next_creation_slots_.emplace(profile.name, 0);
        profiles_.emplace(profile.name, std::move(profile));
    }
}

VSegmentAllocationResult VSegmentManager::Create(
    const std::string& profile_name) {
    auto result = profile_creation_coordinator_.GetOrCreate(
        profile_name, [&] {
            uint64_t slot = 0;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto next = next_creation_slots_.find(profile_name);
                if (next == next_creation_slots_.end())
                    return ManagerError(
                        ErrorCode::INVALID_PARAMS,
                        "unknown vsegment profile: " + profile_name);
                slot = next->second++;
                std::string detail;
                auto persisted =
                    PersistLocked("creation_slot_advanced", &detail);
                if (persisted != ErrorCode::OK) {
                    --next->second;
                    return ManagerError(persisted, std::move(detail));
                }
            }
            return GetOrCreate(profile_name, slot);
        });
    profile_creation_coordinator_.Forget(profile_name);
    return result;
}

std::string VSegmentManager::CreationKey(const std::string& partition_id,
                                         const std::string& profile_name,
                                         uint64_t creation_slot) {
    return partition_id + "/" + profile_name + "/" +
           std::to_string(creation_slot);
}

VSegmentAllocationResult VSegmentManager::GetOrCreate(
    const std::string& profile_name, uint64_t creation_slot) {
    auto profile = profiles_.find(profile_name);
    if (profile == profiles_.end()) {
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    }
    const auto key =
        CreationKey(config_.partition_id, profile_name, creation_slot);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto result = creation_results_.find(key);
        if (result != creation_results_.end()) {
            auto existing = vsegments_.find(result->second);
            if (existing != vsegments_.end())
                return {ErrorCode::OK, existing->second.view, {}};
        }
    }

    auto creation = creation_coordinator_.GetOrCreate(key, [&] {
        std::lock_guard<std::mutex> lock(mutex_);
        auto result = creation_results_.find(key);
        if (result != creation_results_.end()) {
            auto existing = vsegments_.find(result->second);
            if (existing != vsegments_.end())
                return VSegmentAllocationResult{ErrorCode::OK,
                                                existing->second.view, {}};
        }
        const std::string vsegment_id = UuidToString(generate_uuid());
        auto logical = std::make_unique<LogicalRangeAllocator>(
            profile->second.member_extent_size * profile->second.member_count);
        auto allocation =
            physical_allocator_.Allocate(vsegment_id, profile->second);
        if (!allocation) return allocation;
        try {
            creation_results_.emplace(key, vsegment_id);
            vsegments_.emplace(
                vsegment_id,
                ManagedVSegment{profile_name, key, Lifecycle::PREPARING,
                                     allocation.view,
                                     std::move(logical)});
        } catch (...) {
            creation_results_.erase(key);
            vsegments_.erase(vsegment_id);
            physical_allocator_.Release(allocation.view);
            throw;
        }
        std::string detail;
        auto persisted = PersistLocked("vsegment_create_begin", &detail);
        if (persisted != ErrorCode::OK) {
            creation_results_.erase(key);
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
    });
    creation_coordinator_.Forget(key);
    return creation;
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
    managed->second.lifecycle = target;
    const auto persisted = PersistLocked("lifecycle_transition");
    if (persisted != ErrorCode::OK) managed->second.lifecycle = current;
    return persisted;
}

PartitionVSegmentSnapshot VSegmentManager::SnapshotLocked() const {
    PartitionVSegmentSnapshot snapshot;
    snapshot.partition_id = config_.partition_id;
    snapshot.config_generation = config_.config_generation;
    snapshot.next_creation_slots = next_creation_slots_;
    for (const auto& [id, managed] : vsegments_) {
        snapshot.vsegments.push_back(
            {managed.profile_name, managed.creation_key, managed.lifecycle,
             managed.view,
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
    return committer_->Commit(SnapshotLocked(), mutation, detail);
}

ErrorCode VSegmentManager::Restore(
    const PartitionVSegmentSnapshot& snapshot, std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto config_validation = ValidatePartitionConfig(config_, detail);
    if (config_validation != ErrorCode::OK) return config_validation;
    if (!vsegments_.empty()) {
        if (detail) *detail = "restore requires an empty manager";
        return ErrorCode::INVALID_PARAMS;
    }
    if (snapshot.partition_id != config_.partition_id ||
        snapshot.config_generation != config_.config_generation) {
        if (detail) *detail = "snapshot Partition or generation mismatch";
        return ErrorCode::INVALID_VERSION;
    }
    for (const auto& [profile, slot] : snapshot.next_creation_slots) {
        if (!profiles_.count(profile)) {
            if (detail) *detail = "creation cursor has unknown profile";
            return ErrorCode::INVALID_PARAMS;
        }
    }

    PartitionQuotaAllocator verifier(config_);
    std::vector<std::unique_ptr<LogicalRangeAllocator>> logical_allocators;
    std::vector<VSegmentProfile> matching_profiles;
    std::vector<bool> restore_view;
    std::set<std::string> creation_keys;
    for (const auto& state : snapshot.vsegments) {
        if (state.creation_key.empty() ||
            !creation_keys.insert(state.creation_key).second) {
            if (detail) *detail = "duplicate or empty creation_key";
            return ErrorCode::INVALID_PARAMS;
        }
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
        if (state.lifecycle == Lifecycle::PREPARING) {
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
            ManagedVSegment{state.profile_name, state.creation_key,
                            state.lifecycle, state.view,
                            std::move(logical_allocators[index])});
        creation_results_.emplace(state.creation_key,
                                  state.view.vsegment_id);
    }
    next_creation_slots_ = snapshot.next_creation_slots;
    for (const auto& [profile, definition] : profiles_)
        next_creation_slots_.try_emplace(profile, 0);
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
