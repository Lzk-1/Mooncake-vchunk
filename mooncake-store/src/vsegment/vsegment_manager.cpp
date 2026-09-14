#include "vsegment/vsegment_manager.h"

#include <algorithm>

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
                                 std::vector<VSegmentProfile> profiles)
    : config_(std::move(config)), physical_allocator_(config_) {
    for (auto& profile : profiles) {
        profiles_.emplace(profile.name, std::move(profile));
    }
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
        auto existing = vsegments_.find(key);
        if (existing != vsegments_.end())
            return {ErrorCode::OK, existing->second.view, {}};
    }

    return creation_coordinator_.GetOrCreate(key, [&] {
        auto allocation = physical_allocator_.Allocate(key, profile->second);
        if (!allocation) return allocation;
        auto logical = std::make_unique<LogicalRangeAllocator>(
            allocation.view.logical_capacity);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            vsegments_.emplace(
                key, ManagedVSegment{profile_name, allocation.view,
                                     std::move(logical)});
        }
        return allocation;
    });
}

LogicalRangeAllocator* VSegmentManager::FindAllocator(
    const std::string& vsegment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto vsegment = vsegments_.find(vsegment_id);
    return vsegment == vsegments_.end()
               ? nullptr
               : vsegment->second.logical_allocator.get();
}

ReservationResult VSegmentManager::ReservePut(
    const std::string& vsegment_id, const std::string& operation_id,
    uint64_t length) {
    auto* allocator = FindAllocator(vsegment_id);
    if (!allocator)
        return ReservationError(ErrorCode::SEGMENT_NOT_FOUND,
                                "vsegment not found");
    return allocator->Reserve(operation_id, length);
}

ErrorCode VSegmentManager::CommitPut(const std::string& vsegment_id,
                                     const std::string& operation_id,
                                     LogicalRange* range) {
    auto* allocator = FindAllocator(vsegment_id);
    return allocator ? allocator->Commit(operation_id, range)
                     : ErrorCode::SEGMENT_NOT_FOUND;
}

ErrorCode VSegmentManager::AbortPut(const std::string& vsegment_id,
                                    const std::string& operation_id) {
    auto* allocator = FindAllocator(vsegment_id);
    return allocator ? allocator->Abort(operation_id)
                     : ErrorCode::SEGMENT_NOT_FOUND;
}

ErrorCode VSegmentManager::ReleaseObject(const std::string& vsegment_id,
                                         LogicalRange range) {
    auto* allocator = FindAllocator(vsegment_id);
    return allocator ? allocator->Release(range) : ErrorCode::SEGMENT_NOT_FOUND;
}

PartitionVSegmentSnapshot VSegmentManager::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    PartitionVSegmentSnapshot snapshot;
    snapshot.partition_id = config_.partition_id;
    snapshot.config_generation = config_.config_generation;
    for (const auto& [id, managed] : vsegments_) {
        snapshot.vsegments.push_back(
            {managed.profile_name, managed.view,
             managed.logical_allocator->Snapshot()});
    }
    std::sort(snapshot.vsegments.begin(), snapshot.vsegments.end(),
              [](const auto& left, const auto& right) {
                  return left.view.vsegment_id < right.view.vsegment_id;
              });
    return snapshot;
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

    PartitionQuotaAllocator verifier(config_);
    std::vector<std::unique_ptr<LogicalRangeAllocator>> logical_allocators;
    std::vector<VSegmentProfile> matching_profiles;
    for (const auto& state : snapshot.vsegments) {
        auto profile = profiles_.find(state.profile_name);
        if (profile == profiles_.end()) {
            if (detail) *detail = "snapshot view has no matching profile";
            return ErrorCode::INVALID_PARAMS;
        }
        auto validation = verifier.Restore(state.view, profile->second, detail);
        if (validation != ErrorCode::OK) return validation;
        auto logical = std::make_unique<LogicalRangeAllocator>(
            state.view.logical_capacity);
        validation = logical->Restore(state.logical_allocation, detail);
        if (validation != ErrorCode::OK) return validation;
        logical_allocators.push_back(std::move(logical));
        matching_profiles.push_back(profile->second);
    }

    for (size_t index = 0; index < snapshot.vsegments.size(); ++index) {
        const auto& state = snapshot.vsegments[index];
        auto restored = physical_allocator_.Restore(
            state.view, matching_profiles[index], detail);
        if (restored != ErrorCode::OK) return restored;
        vsegments_.emplace(
            state.view.vsegment_id,
            ManagedVSegment{state.profile_name, state.view,
                            std::move(logical_allocators[index])});
    }
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
