#include "vchunk_routing.h"

#include <limits>
#include <utility>

#include <xxhash.h>

#include "etcd_helper.h"

namespace mooncake {

tl::expected<uint32_t, ErrorCode> ComputeVChunkSlot(
    std::string_view tenant_id, std::string_view key, size_t slot_count) {
    if (tenant_id.empty() || key.empty() || slot_count == 0 ||
        slot_count > std::numeric_limits<uint32_t>::max()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::string scoped(tenant_id);
    scoped.push_back('\0');
    scoped.append(key);
    return static_cast<uint32_t>(
        XXH64(scoped.data(), scoped.size(), 0) % slot_count);
}

VChunkStaticRouteTable::VChunkStaticRouteTable(
    uint64_t route_version, std::vector<std::string> slot_owners,
    uint64_t owner_epoch)
    : route_version_(route_version),
      slot_owners_(std::move(slot_owners)),
      owner_epoch_(owner_epoch) {}

ErrorCode VChunkStaticRouteTable::Validate() const {
    if (route_version_ == 0 || owner_epoch_ == 0 || slot_owners_.empty() ||
        slot_owners_.size() > std::numeric_limits<uint32_t>::max()) {
        return ErrorCode::INVALID_PARAMS;
    }
    for (const auto& owner : slot_owners_) {
        if (owner.empty()) return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

tl::expected<VChunkRoute, ErrorCode> VChunkStaticRouteTable::Resolve(
    std::string_view tenant_id, std::string_view key) const {
    if (Validate() != ErrorCode::OK || tenant_id.empty() || key.empty()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto slot = ComputeVChunkSlot(tenant_id, key, slot_owners_.size());
    if (!slot) return tl::make_unexpected(slot.error());
    return VChunkRoute{*slot, slot_owners_[*slot], owner_epoch_,
                       route_version_};
}

ErrorCode VChunkStaticRouteTable::CheckOwner(
    std::string_view local_submaster_id, std::string_view tenant_id,
    std::string_view key, uint64_t request_route_version,
    uint64_t request_owner_epoch) const {
    auto route = Resolve(tenant_id, key);
    if (!route) return route.error();
    if (request_route_version != route->route_version) {
        return ErrorCode::ROUTE_CHANGED;
    }
    if (request_owner_epoch != route->owner_epoch) {
        return ErrorCode::STALE_EPOCH;
    }
    if (local_submaster_id != route->owner_submaster_id) {
        return ErrorCode::NOT_OWNER;
    }
    return ErrorCode::OK;
}

ErrorCode VChunkDynamicRouteTable::ValidateSnapshot(
    const VChunkRouteSnapshot& snapshot) {
    if (snapshot.route_version == 0 || snapshot.slots.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    for (size_t i = 0; i < snapshot.slots.size(); ++i) {
        const auto& route = snapshot.slots[i];
        if (route.slot != i || route.owner_submaster_id.empty() ||
            route.owner_epoch == 0 ||
            (route.state == VChunkSlotState::OWNED &&
             !route.target_submaster_id.empty()) ||
            (route.state != VChunkSlotState::OWNED &&
             route.target_submaster_id.empty()) ||
            (route.state != VChunkSlotState::OWNED &&
             route.target_submaster_id == route.owner_submaster_id) ||
            static_cast<uint8_t>(route.state) >
                static_cast<uint8_t>(VChunkSlotState::TRANSFERRING)) {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    return ErrorCode::OK;
}

ErrorCode VChunkDynamicRouteTable::ApplySnapshot(
    VChunkRouteSnapshot snapshot) {
    if (const auto error = ValidateSnapshot(snapshot); error != ErrorCode::OK) {
        return error;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    if (snapshot.route_version <= snapshot_.route_version) {
        return ErrorCode::ROUTE_CHANGED;
    }
    snapshot_ = std::move(snapshot);
    return ErrorCode::OK;
}

ErrorCode VChunkDynamicRouteTable::BeginTransfer(
    uint32_t slot, std::string target_submaster_id,
    uint64_t next_route_version) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (slot >= snapshot_.slots.size() || target_submaster_id.empty() ||
        next_route_version <= snapshot_.route_version) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto& route = snapshot_.slots[slot];
    if (route.state != VChunkSlotState::OWNED ||
        route.owner_submaster_id == target_submaster_id) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    route.state = VChunkSlotState::DRAINING;
    route.target_submaster_id = std::move(target_submaster_id);
    snapshot_.route_version = next_route_version;
    return ErrorCode::OK;
}

ErrorCode VChunkDynamicRouteTable::MarkTransferring(
    uint32_t slot, uint64_t next_route_version) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (slot >= snapshot_.slots.size() ||
        next_route_version <= snapshot_.route_version) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto& route = snapshot_.slots[slot];
    if (route.state != VChunkSlotState::DRAINING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    route.state = VChunkSlotState::TRANSFERRING;
    snapshot_.route_version = next_route_version;
    return ErrorCode::OK;
}

ErrorCode VChunkDynamicRouteTable::CompleteTransfer(
    uint32_t slot, uint64_t next_owner_epoch, uint64_t next_route_version) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (slot >= snapshot_.slots.size() || next_owner_epoch == 0 ||
        next_route_version <= snapshot_.route_version) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto& route = snapshot_.slots[slot];
    if (route.state != VChunkSlotState::TRANSFERRING ||
        next_owner_epoch <= route.owner_epoch) {
        return ErrorCode::STALE_EPOCH;
    }
    route.owner_submaster_id = std::move(route.target_submaster_id);
    route.target_submaster_id.clear();
    route.owner_epoch = next_owner_epoch;
    route.state = VChunkSlotState::OWNED;
    snapshot_.route_version = next_route_version;
    return ErrorCode::OK;
}

ErrorCode VChunkDynamicRouteTable::AbortTransfer(
    uint32_t slot, uint64_t next_route_version) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (slot >= snapshot_.slots.size() ||
        next_route_version <= snapshot_.route_version) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto& route = snapshot_.slots[slot];
    // Once TRANSFERRING is durable, ownership/resource side effects may have
    // happened. Only DRAINING is guaranteed to be safe to roll back.
    if (route.state != VChunkSlotState::DRAINING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    route.state = VChunkSlotState::OWNED;
    route.target_submaster_id.clear();
    snapshot_.route_version = next_route_version;
    return ErrorCode::OK;
}

tl::expected<VChunkSlotRoute, ErrorCode> VChunkDynamicRouteTable::Resolve(
    uint32_t slot) const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (slot >= snapshot_.slots.size()) {
        return tl::unexpected(ErrorCode::SHARD_INDEX_OUT_OF_RANGE);
    }
    return snapshot_.slots[slot];
}

VChunkRouteSnapshot VChunkDynamicRouteTable::Snapshot() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return snapshot_;
}

uint64_t VChunkDynamicRouteTable::Version() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return snapshot_.route_version;
}

tl::expected<VChunkRouteSnapshot, ErrorCode>
InMemoryVChunkRouteStore::Load() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!snapshot_) return tl::make_unexpected(ErrorCode::ETCD_KEY_NOT_EXIST);
    return *snapshot_;
}

ErrorCode InMemoryVChunkRouteStore::Publish(
    uint64_t expected_version, const VChunkRouteSnapshot& snapshot) {
    VChunkDynamicRouteTable validator;
    if (validator.ApplySnapshot(snapshot) != ErrorCode::OK ||
        snapshot.route_version <= expected_version) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    const uint64_t current = snapshot_ ? snapshot_->route_version : 0;
    if (current != expected_version) return ErrorCode::ROUTE_CHANGED;
    snapshot_ = snapshot;
    return ErrorCode::OK;
}

EtcdVChunkRouteStore::EtcdVChunkRouteStore(std::string endpoints,
                                           std::string cluster_id) {
    if (endpoints.empty() || cluster_id.empty() ||
        cluster_id.find('/') != std::string::npos) {
        connection_error_ = ErrorCode::INVALID_PARAMS;
        return;
    }
    route_key_ = "/mooncake/vchunk/v1/clusters/" + cluster_id +
                 "/routes/current";
#ifdef STORE_USE_ETCD
    connection_error_ = EtcdHelper::ConnectToEtcdStoreClient(endpoints);
#else
    connection_error_ = ErrorCode::ETCD_OPERATION_ERROR;
#endif
}

tl::expected<VChunkRouteSnapshot, ErrorCode> EtcdVChunkRouteStore::Load() {
    if (connection_error_ != ErrorCode::OK) {
        return tl::make_unexpected(connection_error_);
    }
    std::string value;
    EtcdRevisionId revision = 0;
    const auto error = EtcdHelper::Get(route_key_.data(), route_key_.size(),
                                       value, revision);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    VChunkRouteSnapshot snapshot;
    if (struct_pack::deserialize_to(snapshot, value) != struct_pack::errc::ok) {
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }
    VChunkDynamicRouteTable validator;
    if (validator.ApplySnapshot(snapshot) != ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }
    return snapshot;
}

ErrorCode EtcdVChunkRouteStore::Publish(
    uint64_t expected_version, const VChunkRouteSnapshot& snapshot) {
    if (connection_error_ != ErrorCode::OK) return connection_error_;
    VChunkDynamicRouteTable validator;
    if (validator.ApplySnapshot(snapshot) != ErrorCode::OK ||
        snapshot.route_version <= expected_version) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto encoded = struct_pack::serialize(snapshot);
    const std::string next(encoded.begin(), encoded.end());
    if (expected_version == 0) {
        const auto error = EtcdHelper::TxnCompareAndPut(
            {{route_key_, EtcdHelper::TxnCompareKind::kKeyNotExists, {}}},
            {{route_key_, next}});
        return error == ErrorCode::ETCD_TRANSACTION_FAIL
                   ? ErrorCode::ROUTE_CHANGED
                   : error;
    }

    std::string current;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::Get(route_key_.data(), route_key_.size(), current,
                                 revision);
    if (error != ErrorCode::OK) return error;
    VChunkRouteSnapshot current_snapshot;
    if (struct_pack::deserialize_to(current_snapshot, current) !=
            struct_pack::errc::ok ||
        current_snapshot.route_version != expected_version) {
        return ErrorCode::ROUTE_CHANGED;
    }
    error = EtcdHelper::TxnCompareAndPut(
        {{route_key_, EtcdHelper::TxnCompareKind::kValueEquals, current}},
        {{route_key_, next}});
    return error == ErrorCode::ETCD_TRANSACTION_FAIL
               ? ErrorCode::ROUTE_CHANGED
               : error;
}

}  // namespace mooncake
