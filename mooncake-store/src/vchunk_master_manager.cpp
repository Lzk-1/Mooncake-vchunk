#include "vchunk_master_manager.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "types.h"

namespace mooncake {

namespace {

int64_t CleanupDeadline(int64_t now_ms, uint64_t timeout_ms) {
    if (timeout_ms > static_cast<uint64_t>(
                         std::numeric_limits<int64_t>::max() - now_ms)) {
        return std::numeric_limits<int64_t>::max();
    }
    return now_ms + static_cast<int64_t>(timeout_ms);
}

}  // namespace

VChunkMasterManager::VChunkMasterManager(
    VChunkConfig config, std::shared_ptr<VChunkMetadataStore> metadata_store,
    std::shared_ptr<VChunkMetrics> metrics,
    std::shared_ptr<VChunkRouteStore> route_store)
    : config_(std::move(config)),
      metadata_store_(metadata_store ? std::move(metadata_store)
                                     : std::make_shared<
                                           InMemoryVChunkMetadataStore>()),
      metrics_(metrics ? std::move(metrics)
                       : std::make_shared<VChunkMetrics>()),
      route_store_(std::move(route_store)) {
    if (!config_.static_slot_owners.empty()) {
        static_routes_.emplace(config_.route_version,
                               config_.static_slot_owners,
                               config_.owner_epoch);
        VChunkRouteSnapshot snapshot;
        snapshot.route_version = config_.route_version;
        snapshot.slots.reserve(config_.static_slot_owners.size());
        for (size_t slot = 0; slot < config_.static_slot_owners.size(); ++slot) {
            snapshot.slots.push_back(
                {static_cast<uint32_t>(slot), VChunkSlotState::OWNED,
                 config_.static_slot_owners[slot], {}, config_.owner_epoch});
        }
        if (route_store_) {
            auto persisted = route_store_->Load();
            if (persisted) {
                if (persisted->slots.size() != static_routes_->SlotCount() ||
                    persisted->route_version < snapshot.route_version) {
                    if (route_store_->IsPersistent()) {
                        throw std::runtime_error(
                            "persistent vchunk route is incompatible");
                    }
                } else {
                    snapshot = std::move(*persisted);
                }
            } else if (persisted.error() == ErrorCode::ETCD_KEY_NOT_EXIST) {
                const auto initialized = route_store_->Publish(0, snapshot);
                if (initialized != ErrorCode::OK &&
                    route_store_->IsPersistent()) {
                    throw std::runtime_error(
                        "failed to initialize persistent vchunk route");
                }
            } else if (route_store_->IsPersistent()) {
                throw std::runtime_error(
                    "failed to load persistent vchunk route");
            }
        }
        dynamic_routes_.ApplySnapshot(std::move(snapshot));
    } else if (route_store_) {
        LOG(WARNING) << "vchunk route store ignored without static slots";
    }
}

std::string VChunkMasterManager::ScopedKey(const TenantId& tenant_id,
                                           const std::string& key) {
    return tenant_id.MakeScopedKey(key);
}

ErrorCode VChunkMasterManager::CheckLeaderEpoch(
    uint64_t expected_leader_epoch) const {
    std::function<bool()> membership_check;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        membership_check = membership_check_;
    }
    if (!accepts_mutations_.load() ||
        (membership_check && !membership_check())) {
        return ErrorCode::NOT_LEADER;
    }
    const auto current = leader_epoch_.load();
    if (expected_leader_epoch != 0 && expected_leader_epoch != current) {
        return ErrorCode::STALE_EPOCH;
    }
    return ErrorCode::OK;
}

ErrorCode VChunkMasterManager::CheckStaticOwner(
    const TenantId& tenant_id, const std::string& key) const {
    auto route = ResolveRoute(tenant_id, key);
    if (!route) return route.error();
    return route->owner_submaster_id.empty() ||
                   route->owner_submaster_id == config_.submaster_id
               ? ErrorCode::OK
               : ErrorCode::NOT_OWNER;
}

tl::expected<VChunkRoute, ErrorCode> VChunkMasterManager::ResolveRoute(
    const TenantId& tenant_id, const std::string& key) const {
    if (!static_routes_) return VChunkRoute{};
    auto route = static_routes_->Resolve(tenant_id.value(), key);
    if (!route) return route.error();
    auto dynamic = dynamic_routes_.Resolve(route->slot);
    if (!dynamic) return dynamic.error();
    if (dynamic->state != VChunkSlotState::OWNED) {
        return tl::make_unexpected(ErrorCode::ROUTE_CHANGED);
    }
    route->owner_submaster_id = dynamic->owner_submaster_id;
    route->owner_epoch = dynamic->owner_epoch;
    route->route_version = dynamic_routes_.Version();
    return route;
}

ErrorCode VChunkMasterManager::ApplyRouteSnapshot(
    VChunkRouteSnapshot snapshot) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    const auto current = dynamic_routes_.Snapshot();
    if (snapshot.slots.size() == current.slots.size()) {
        for (size_t slot = 0; slot < snapshot.slots.size(); ++slot) {
            if (snapshot.slots[slot].owner_submaster_id !=
                    current.slots[slot].owner_submaster_id &&
                SlotHasEntries(static_cast<uint32_t>(slot))) {
                return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            }
        }
    }
    return PublishRouteSnapshot(std::move(snapshot));
}

ErrorCode VChunkMasterManager::PublishRouteSnapshot(
    VChunkRouteSnapshot snapshot) {
    if (!static_routes_ || snapshot.slots.size() != static_routes_->SlotCount()) {
        return ErrorCode::INVALID_PARAMS;
    }
    VChunkDynamicRouteTable validator;
    if (validator.ApplySnapshot(snapshot) != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (route_store_) {
        const auto error =
            route_store_->Publish(dynamic_routes_.Version(), snapshot);
        if (error != ErrorCode::OK) return error;
    }
    return dynamic_routes_.ApplySnapshot(std::move(snapshot));
}

bool VChunkMasterManager::SlotHasEntries(uint32_t slot) const {
    std::lock_guard<std::mutex> guard(mutex_);
    for (const auto& [_, entry] : entries_) {
        auto route = static_routes_->Resolve(entry->record.tenant_id,
                                             entry->record.key);
        if (route && route->slot == slot) return true;
    }
    return false;
}

ErrorCode VChunkMasterManager::BeginSlotTransfer(
    uint32_t slot, std::string target_submaster_id,
    uint64_t next_route_version) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    auto snapshot = dynamic_routes_.Snapshot();
    if (slot >= snapshot.slots.size()) return ErrorCode::INVALID_PARAMS;
    if (snapshot.slots[slot].owner_submaster_id != config_.submaster_id) {
        return ErrorCode::NOT_OWNER;
    }
    VChunkDynamicRouteTable next;
    if (next.ApplySnapshot(snapshot) != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto error = next.BeginTransfer(slot, std::move(target_submaster_id),
                                          next_route_version);
    if (error != ErrorCode::OK) return error;
    return PublishRouteSnapshot(next.Snapshot());
}

ErrorCode VChunkMasterManager::MarkSlotTransferring(
    uint32_t slot, uint64_t next_route_version) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    // Until allocator ownership and object bytes can be moved atomically, only
    // empty slots may cross the point where rollback is no longer safe.
    if (SlotHasEntries(slot)) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    auto snapshot = dynamic_routes_.Snapshot();
    VChunkDynamicRouteTable next;
    if (next.ApplySnapshot(snapshot) != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto error = next.MarkTransferring(slot, next_route_version);
    if (error != ErrorCode::OK) return error;
    return PublishRouteSnapshot(next.Snapshot());
}

ErrorCode VChunkMasterManager::CompleteSlotTransfer(
    uint32_t slot, uint64_t next_owner_epoch, uint64_t next_route_version) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    if (SlotHasEntries(slot)) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    auto snapshot = dynamic_routes_.Snapshot();
    VChunkDynamicRouteTable next;
    if (next.ApplySnapshot(snapshot) != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto error =
        next.CompleteTransfer(slot, next_owner_epoch, next_route_version);
    if (error != ErrorCode::OK) return error;
    return PublishRouteSnapshot(next.Snapshot());
}

ErrorCode VChunkMasterManager::AbortSlotTransfer(
    uint32_t slot, uint64_t next_route_version) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    auto snapshot = dynamic_routes_.Snapshot();
    VChunkDynamicRouteTable next;
    if (next.ApplySnapshot(snapshot) != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto error = next.AbortTransfer(slot, next_route_version);
    if (error != ErrorCode::OK) return error;
    return PublishRouteSnapshot(next.Snapshot());
}

void VChunkMasterManager::SetDurabilitySink(DurabilitySink sink) {
    std::lock_guard<std::mutex> guard(mutex_);
    durability_sink_ = std::move(sink);
}

void VChunkMasterManager::SetMembershipCheck(std::function<bool()> check) {
    std::lock_guard<std::mutex> guard(mutex_);
    membership_check_ = std::move(check);
}

ErrorCode VChunkMasterManager::PersistEvent(
    VChunkHAEventType type, const VChunkMetadataRecord& record) const {
    return durability_sink_ ? durability_sink_(type, record) : ErrorCode::OK;
}

ErrorCode VChunkMasterManager::ActivateLeaderEpoch(uint64_t leader_epoch) {
    if (leader_epoch == 0) return ErrorCode::INVALID_PARAMS;
    std::lock_guard<std::mutex> guard(mutex_);
    auto current = leader_epoch_.load();
    while (leader_epoch > current &&
           !leader_epoch_.compare_exchange_weak(current, leader_epoch)) {
    }
    if (leader_epoch < current) return ErrorCode::STALE_EPOCH;
    accepts_mutations_.store(true);
    return ErrorCode::OK;
}

void VChunkMasterManager::DeactivateLeader() {
    accepts_mutations_.store(false);
}

ErrorCode VChunkMasterManager::PublishRecoveryView(VChunkRecoveryView view) {
    if (view.leader_epoch == 0) return ErrorCode::INVALID_PARAMS;
    std::unordered_map<std::string, std::shared_ptr<Entry>> recovered;
    recovered.reserve(view.entries.size());
    for (auto& source : view.entries) {
        if (source.record.leader_epoch != view.leader_epoch ||
            ValidateVChunkMetadata(source.record, config_) != ErrorCode::OK ||
            source.claims.size() != source.record.slices.size()) {
            return ErrorCode::INVALID_PARAMS;
        }
        auto entry = std::make_shared<Entry>();
        entry->record = std::move(source.record);
        entry->buffers = std::move(source.claims);
        if (entry->record.status == VChunkStatus::CREATING) {
            entry->cleanup_deadline_ms = CleanupDeadline(
                entry->record.last_updated_at_ms, config_.creating_timeout_ms);
        } else if (entry->record.status == VChunkStatus::RELEASING) {
            entry->cleanup_deadline_ms = CleanupDeadline(
                entry->record.last_updated_at_ms,
                config_.releasing_timeout_ms);
        }
        const auto key = ScopedKey(TenantId(entry->record.tenant_id),
                                   entry->record.key);
        if (!recovered.emplace(key, std::move(entry)).second) {
            return ErrorCode::OBJECT_ALREADY_EXISTS;
        }
    }

    std::lock_guard<std::mutex> guard(mutex_);
    if (view.leader_epoch < leader_epoch_.load()) {
        return ErrorCode::STALE_EPOCH;
    }
    leader_epoch_.store(view.leader_epoch);
    entries_.swap(recovered);
    pending_puts_.clear();
    reaper_cursor_key_.clear();
    accepts_mutations_.store(true);
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

tl::expected<VChunkMetadataRecord, ErrorCode> VChunkMasterManager::PutStart(
    const AllocatorManager& allocator_manager, const TenantId& tenant_id,
    const std::string& key, uint64_t total_size, bool is_ssd_segment,
    int64_t now_ms, const std::set<std::string>& excluded_segments,
    uint64_t expected_leader_epoch) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    if (const auto error = CheckLeaderEpoch(expected_leader_epoch);
        error != ErrorCode::OK) {
        return tl::make_unexpected(error);
    }
    if (const auto error = CheckStaticOwner(tenant_id, key);
        error != ErrorCode::OK) {
        return tl::make_unexpected(error);
    }
    const uint64_t operation_epoch = leader_epoch_.load();
    if (!config_.enabled || config_.Validate() != ErrorCode::OK ||
        !tenant_id.IsValid() || key.empty() || total_size == 0 || now_ms < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // The current version owns buffers from SegmentManager and supports
    // memory segments only. SSD/NoF routing is introduced in a later stage.
    if (is_ssd_segment) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto scoped_key = ScopedKey(tenant_id, key);
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (entries_.contains(scoped_key) || pending_puts_.contains(scoped_key)) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        size_t creating = 0;
        for (const auto& [_, entry] : entries_) {
            creating += entry->record.status == VChunkStatus::CREATING;
        }
        if (creating + pending_puts_.size() >= config_.max_creating_objects) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        pending_puts_.insert(scoped_key);
    }

    const auto slice_size_level =
        SelectVChunkSliceSize(total_size, is_ssd_segment);
    const uint64_t slice_size = SliceSizeLevelToBytes(slice_size_level);
    if (total_size > std::numeric_limits<uint64_t>::max() - (slice_size - 1) ||
        (total_size + slice_size - 1) / slice_size >
            config_.max_slice_count) {
        ReleasePendingPut(scoped_key);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto allocation = AllocateVChunk(allocator_manager, total_size,
                                     slice_size_level, excluded_segments,
                                     config_.replica_num);
    if (!allocation) {
        ReleasePendingPut(scoped_key);
        metrics_->AddAllocationFailure();
        return tl::make_unexpected(allocation.error());
    }

    auto entry = std::make_shared<Entry>();
    auto& record = entry->record;
    record.vchunk_id = UuidToString(generate_uuid());
    record.tenant_id = tenant_id.value();
    record.key = key;
    record.total_size = total_size;
    record.slice_count = static_cast<uint32_t>(
        allocation->allocations.size() / allocation->replica_num);
    record.slice_size_level = slice_size_level;
    record.row_size = static_cast<uint32_t>(allocation->row_size);
    record.replica_num = allocation->replica_num;
    record.slice_groups = allocation->slice_groups;
    record.status = VChunkStatus::CREATING;
    record.created_at_ms = now_ms;
    record.last_updated_at_ms = now_ms;
    entry->cleanup_deadline_ms =
        CleanupDeadline(now_ms, config_.creating_timeout_ms);
    record.metadata_version = 1;
    record.leader_epoch = operation_epoch;
    auto route = ResolveRoute(tenant_id, key);
    if (!route) {
        ReleasePendingPut(scoped_key);
        return tl::make_unexpected(route.error());
    }
    record.owner_slot = route->slot;
    record.owner_submaster_id = std::move(route->owner_submaster_id);
    record.owner_epoch = route->owner_epoch;
    record.route_version = route->route_version;
    record.slices.reserve(record.slice_count);
    entry->buffers.reserve(record.slice_count);
    for (auto& allocated : allocation->allocations) {
        VCSliceDescriptor descriptor;
        descriptor.slice_index = allocated.slice_index;
        descriptor.target_segment_name = allocated.segment_name;
        descriptor.target_offset = allocated.target_offset;
        descriptor.logical_length = allocated.logical_length;
        descriptor.allocated_length = allocated.allocated_length;
        descriptor.status = VCSliceStatus::PENDING;
        descriptor.replica_group_id = allocated.slice_index;
        descriptor.replica_index = allocated.replica_index;
        descriptor.segment_instance_id = allocated.segment_instance_id;
        descriptor.allocation_generation = record.metadata_version;
        record.slices.push_back(std::move(descriptor));
        entry->buffers.push_back(std::move(allocated.buffer));
    }
    const auto serialized = SerializeVChunkMetadata(record, config_);
    if (!serialized) {
        ReleasePendingPut(scoped_key);
        return tl::make_unexpected(serialized.error());
    }
    if (const auto error = CheckLeaderEpoch(operation_epoch);
        error != ErrorCode::OK) {
        ReleasePendingPut(scoped_key);
        return tl::make_unexpected(error);
    }
    if (const auto error = PersistEvent(VChunkHAEventType::CREATE, record);
        error != ErrorCode::OK) {
        ReleasePendingPut(scoped_key);
        return tl::make_unexpected(error);
    }
    if (const auto error = metadata_store_->Put(record);
        error != ErrorCode::OK) {
        ReleasePendingPut(scoped_key);
        return tl::make_unexpected(error);
    }

    std::lock_guard<std::mutex> guard(mutex_);
    pending_puts_.erase(scoped_key);
    const auto snapshot = record;
    entries_.emplace(scoped_key, std::move(entry));
    metrics_->AddSlices(snapshot.slice_count);
    metrics_->ObserveLayout(snapshot);
    metrics_->AddMetadataBytes(serialized->size());
    RefreshStateMetricsLocked();
    return snapshot;
}

ErrorCode VChunkMasterManager::PutEnd(const TenantId& tenant_id,
                                      const std::string& key,
                                      const std::string& vchunk_id,
                                      int64_t now_ms,
                                      uint64_t expected_leader_epoch,
                                      const std::vector<uint64_t>&
                                          slice_checksums) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    if (const auto error = CheckLeaderEpoch(expected_leader_epoch);
        error != ErrorCode::OK) {
        return error;
    }
    if (const auto error = CheckStaticOwner(tenant_id, key);
        error != ErrorCode::OK) {
        return error;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    auto& record = it->second->record;
    if (record.vchunk_id != vchunk_id) {
        return ErrorCode::INVALID_VERSION;
    }
    if (record.leader_epoch != leader_epoch_.load()) {
        return ErrorCode::STALE_EPOCH;
    }
    if (record.status == VChunkStatus::ACTIVE) {
        return ErrorCode::OK;
    }
    if (ValidateVChunkTransition(record.status, VChunkStatus::ACTIVE) !=
            ErrorCode::OK ||
        now_ms < record.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto durable = record;
    if (!slice_checksums.empty() &&
        slice_checksums.size() != durable.slices.size()) {
        return ErrorCode::INVALID_PARAMS;
    }
    for (size_t i = 0; i < durable.slices.size(); ++i) {
        auto& slice = durable.slices[i];
        slice.status = VCSliceStatus::COMPLETED;
        if (!slice_checksums.empty()) {
            slice.content_checksum = slice_checksums[i];
        }
    }
    durable.status = VChunkStatus::ACTIVE;
    durable.last_updated_at_ms = now_ms;
    ++durable.metadata_version;
    if (const auto error = PersistEvent(VChunkHAEventType::ACTIVATE, durable);
        error != ErrorCode::OK) {
        return error;
    }
    if (const auto error = metadata_store_->Put(durable);
        error != ErrorCode::OK) {
        return error;
    }
    record = std::move(durable);
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

ErrorCode VChunkMasterManager::PutRevoke(const TenantId& tenant_id,
                                         const std::string& key,
                                         const std::string& vchunk_id,
                                         uint64_t expected_leader_epoch) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    if (const auto error = CheckLeaderEpoch(expected_leader_epoch);
        error != ErrorCode::OK) {
        return error;
    }
    if (const auto error = CheckStaticOwner(tenant_id, key);
        error != ErrorCode::OK) {
        return error;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return ErrorCode::OK;
    }
    if (it->second->record.vchunk_id != vchunk_id) {
        return ErrorCode::INVALID_VERSION;
    }
    if (it->second->record.leader_epoch != leader_epoch_.load()) {
        return ErrorCode::STALE_EPOCH;
    }
    if (it->second->record.status != VChunkStatus::CREATING &&
        it->second->record.status != VChunkStatus::FAILED) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (const auto error =
            PersistEvent(VChunkHAEventType::REVOKE, it->second->record);
        error != ErrorCode::OK) {
        return error;
    }
    metrics_->AddCleanupAttempt();
    ++it->second->cleanup_attempts;
    if (const auto error = metadata_store_->Remove(it->second->record);
        error != ErrorCode::OK) {
        it->second->cleanup_pending = true;
        metrics_->AddCleanupFailure();
        RefreshStateMetricsLocked();
        return error;
    }
    entries_.erase(it);
    metrics_->AddRollback();
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

tl::expected<VChunkMetadataRecord, ErrorCode> VChunkMasterManager::Get(
    const TenantId& tenant_id, const std::string& key) const {
    auto handle = AcquireRead(tenant_id, key);
    if (!handle) {
        return tl::make_unexpected(handle.error());
    }
    return handle->record();
}

tl::expected<VChunkMasterManager::ReadHandle, ErrorCode>
VChunkMasterManager::AcquireRead(const TenantId& tenant_id,
                                 const std::string& key) const {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    if (!accepts_mutations_.load()) {
        return tl::make_unexpected(ErrorCode::NOT_LEADER);
    }
    if (const auto error = CheckStaticOwner(tenant_id, key);
        error != ErrorCode::OK) {
        return tl::make_unexpected(error);
    }
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (it->second->record.status != VChunkStatus::ACTIVE) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    ReadHandle handle;
    handle.record_ = it->second->record;
    handle.lifetime_ = it->second;
    return handle;
}

ErrorCode VChunkMasterManager::Remove(const TenantId& tenant_id,
                                      const std::string& key,
                                      int64_t now_ms,
                                      uint64_t expected_leader_epoch) {
    std::lock_guard<std::mutex> route_guard(route_mutex_);
    if (const auto error = CheckLeaderEpoch(expected_leader_epoch);
        error != ErrorCode::OK) {
        return error;
    }
    if (const auto error = CheckStaticOwner(tenant_id, key);
        error != ErrorCode::OK) {
        return error;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    const auto scoped_key = ScopedKey(tenant_id, key);
    const auto it = entries_.find(scoped_key);
    if (it == entries_.end()) {
        return ErrorCode::OK;
    }
    auto& record = it->second->record;
    if (record.leader_epoch != leader_epoch_.load()) {
        return ErrorCode::STALE_EPOCH;
    }
    if ((record.status != VChunkStatus::ACTIVE &&
         record.status != VChunkStatus::RELEASING) ||
        now_ms < record.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.status == VChunkStatus::ACTIVE) {
        auto releasing = record;
        releasing.status = VChunkStatus::RELEASING;
        releasing.last_updated_at_ms = now_ms;
        ++releasing.metadata_version;
        if (const auto error =
                PersistEvent(VChunkHAEventType::BEGIN_RELEASE, releasing);
            error != ErrorCode::OK) {
            return error;
        }
        if (const auto error = metadata_store_->Put(releasing);
            error != ErrorCode::OK) {
            return error;
        }
        record = std::move(releasing);
        it->second->cleanup_deadline_ms =
            CleanupDeadline(now_ms, config_.releasing_timeout_ms);
    }
    auto released = record;
    released.status = VChunkStatus::RELEASED;
    ++released.metadata_version;
    if (const auto error = PersistEvent(VChunkHAEventType::RELEASED, released);
        error != ErrorCode::OK) {
        return error;
    }
    metrics_->AddCleanupAttempt();
    ++it->second->cleanup_attempts;
    if (const auto error = metadata_store_->Remove(record);
        error != ErrorCode::OK) {
        it->second->cleanup_pending = true;
        metrics_->AddCleanupFailure();
        RefreshStateMetricsLocked();
        return error;
    }
    entries_.erase(it);
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

ErrorCode VChunkMasterManager::Recover(int64_t now_ms,
                                       OwnershipPredicate owns) {
    if (const auto error = CheckLeaderEpoch(0); error != ErrorCode::OK) {
        return error;
    }
    if (now_ms < 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto records = metadata_store_->List();
    if (!records) {
        return records.error();
    }
    std::lock_guard<std::mutex> guard(mutex_);
    // Validate the complete snapshot before mutating the store so recovery is
    // deterministic even when List() returns records in a different order.
    for (const auto& record : *records) {
        if (owns && !owns(record)) {
            continue;
        }
        const auto validation = ValidateVChunkMetadata(record, config_);
        if (validation != ErrorCode::OK) {
            return validation;
        }
        // Allocator reservations are process-local. A persisted ACTIVE record
        // must never be published until its exact ranges have been reserved
        // again, otherwise new allocations can overlap it.
        if (record.status == VChunkStatus::ACTIVE) {
            return ErrorCode::REPLICA_IS_GONE;
        }
    }
    for (const auto& record : *records) {
        if (owns && !owns(record)) {
            continue;
        }
        // CREATING records cannot be resumed safely either: their buffers were
        // owned by the previous process. Treat all incomplete writes as stale.
        if (record.status == VChunkStatus::CREATING ||
            record.status == VChunkStatus::RECOVERING ||
            record.status == VChunkStatus::ABANDONED ||
            record.status == VChunkStatus::RELEASING ||
            record.status == VChunkStatus::RELEASED ||
            record.status == VChunkStatus::FAILED) {
            const auto error = metadata_store_->Remove(record);
            if (error != ErrorCode::OK) {
                return error;
            }
            continue;
        }
    }
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

tl::expected<size_t, ErrorCode> VChunkMasterManager::ReapExpired(
    int64_t now_ms, size_t max_scan, OwnershipPredicate owns) {
    if (const auto error = CheckLeaderEpoch(0); error != ErrorCode::OK) {
        return tl::make_unexpected(error);
    }
    if (now_ms < 0 || max_scan == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> guard(mutex_);
    size_t scanned = 0;
    size_t removed = 0;
    if (entries_.empty()) {
        reaper_cursor_key_.clear();
        return removed;
    }
    auto it = reaper_cursor_key_.empty() ? entries_.begin()
                                         : entries_.find(reaper_cursor_key_);
    if (it == entries_.end()) it = entries_.begin();
    const size_t scan_limit = std::min(max_scan, entries_.size());
    while (!entries_.empty() && scanned < scan_limit) {
        ++scanned;
        auto next = std::next(it);
        if (next == entries_.end()) next = entries_.begin();
        const auto& record = it->second->record;
        if (record.leader_epoch != leader_epoch_.load() ||
            (owns && !owns(record))) {
            it = next;
            continue;
        }
        const bool expired = it->second->cleanup_deadline_ms > 0 &&
                             now_ms >= it->second->cleanup_deadline_ms &&
                             (record.status == VChunkStatus::CREATING ||
                              record.status == VChunkStatus::RELEASING);
        if (!expired) {
            it = next;
            continue;
        }
        auto cleanup = record;
        const auto event_type = record.status == VChunkStatus::CREATING
                                    ? VChunkHAEventType::REVOKE
                                    : VChunkHAEventType::RELEASED;
        if (event_type == VChunkHAEventType::RELEASED) {
            cleanup.status = VChunkStatus::RELEASED;
            ++cleanup.metadata_version;
        }
        if (const auto error = PersistEvent(event_type, cleanup);
            error != ErrorCode::OK) {
            return tl::make_unexpected(error);
        }
        it->second->cleanup_pending = true;
        metrics_->AddCleanupAttempt();
        ++it->second->cleanup_attempts;
        if (const auto error = metadata_store_->Remove(record);
            error != ErrorCode::OK) {
            metrics_->AddCleanupFailure();
            if (it->second->cleanup_attempts < config_.cleanup_max_attempts) {
                it->second->cleanup_deadline_ms =
                    CleanupDeadline(now_ms, config_.cleanup_retry_backoff_ms);
            } else {
                it->second->cleanup_deadline_ms = 0;
            }
            RefreshStateMetricsLocked();
            return tl::make_unexpected(error);
        }
        entries_.erase(it);
        ++removed;
        metrics_->AddRollback();
        if (entries_.empty()) break;
        it = next;
    }
    reaper_cursor_key_ = entries_.empty() ? std::string() : it->first;
    RefreshStateMetricsLocked();
    return removed;
}

VChunkMetricsSnapshot VChunkMasterManager::MetricsSnapshot() const {
    return metrics_->Snapshot();
}

tl::expected<VChunkScrubReport, ErrorCode> VChunkMasterManager::Scrub() const {
    std::unordered_map<std::string, VChunkMetadataRecord> runtime;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        runtime.reserve(entries_.size());
        for (const auto& [key, entry] : entries_) {
            runtime.emplace(key, entry->record);
        }
    }

    VChunkScrubReport report;
    report.runtime_records = runtime.size();
    struct Range {
        std::string segment;
        uint64_t offset;
        uint64_t length;
    };
    std::vector<Range> ranges;
    for (const auto& [_, record] : runtime) {
        if (ValidateVChunkMetadata(record, config_) != ErrorCode::OK) {
            ++report.invalid_records;
            continue;
        }
        if (record.route_version != 0 && !config_.submaster_id.empty() &&
            record.owner_submaster_id != config_.submaster_id) {
            ++report.ownership_mismatches;
        }
        for (const auto& slice : record.slices) {
            ranges.push_back({slice.target_segment_name, slice.target_offset,
                              slice.allocated_length});
        }
    }
    std::sort(ranges.begin(), ranges.end(), [](const auto& lhs,
                                                const auto& rhs) {
        return std::tie(lhs.segment, lhs.offset) <
               std::tie(rhs.segment, rhs.offset);
    });
    for (size_t i = 1; i < ranges.size(); ++i) {
        const auto& previous = ranges[i - 1];
        const auto& current = ranges[i];
        if (previous.segment == current.segment &&
            previous.offset <=
                std::numeric_limits<uint64_t>::max() - previous.length &&
            previous.offset + previous.length > current.offset) {
            ++report.overlapping_ranges;
        }
    }

    const auto observe = [&] {
        metrics_->ObserveScrub(
            report.invalid_records + report.missing_persistent_records +
            report.stale_persistent_records +
            report.unexpected_persistent_records +
            report.ownership_mismatches + report.overlapping_ranges);
    };
    if (!metadata_store_->IsPersistent()) {
        observe();
        return report;
    }
    auto stored = metadata_store_->List();
    if (!stored) {
        metrics_->AddScrubFailure();
        return tl::make_unexpected(stored.error());
    }
    report.persistent_records = stored->size();
    std::unordered_map<std::string, VChunkMetadataRecord> persistent;
    persistent.reserve(stored->size());
    for (auto& record : *stored) {
        const auto key = ScopedKey(TenantId(record.tenant_id), record.key);
        if (!persistent.emplace(key, std::move(record)).second) {
            ++report.invalid_records;
        }
    }
    for (const auto& [key, record] : runtime) {
        const auto found = persistent.find(key);
        if (found == persistent.end()) {
            ++report.missing_persistent_records;
            continue;
        }
        if (found->second.vchunk_id != record.vchunk_id ||
            found->second.metadata_version != record.metadata_version ||
            found->second.leader_epoch != record.leader_epoch) {
            ++report.stale_persistent_records;
        }
        persistent.erase(found);
    }
    report.unexpected_persistent_records = persistent.size();
    observe();
    return report;
}

void VChunkMasterManager::RefreshStateMetricsLocked() {
    std::array<uint64_t, 7> counts{};
    uint64_t allocated_bytes = 0;
    uint64_t pending_cleanup = 0;
    for (const auto& [_, entry] : entries_) {
        ++counts[static_cast<size_t>(entry->record.status)];
        for (const auto& buffer : entry->buffers) {
            if (buffer) allocated_bytes += buffer->size();
        }
        pending_cleanup += entry->cleanup_pending;
    }
    for (size_t i = 0; i < counts.size(); ++i) {
        metrics_->SetStateCount(static_cast<VChunkStatus>(i), counts[i]);
    }
    metrics_->SetAllocatedBytes(allocated_bytes);
    metrics_->SetPendingCleanup(pending_cleanup);
}

size_t VChunkMasterManager::SizeForTesting() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return entries_.size();
}

void VChunkMasterManager::ReleasePendingPut(const std::string& scoped_key) {
    std::lock_guard<std::mutex> guard(mutex_);
    pending_puts_.erase(scoped_key);
}

}  // namespace mooncake
