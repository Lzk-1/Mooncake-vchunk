#include "vchunk_control_plane.h"

#include "master_client.h"
#include "master_service.h"


namespace mooncake {

namespace {

class RpcReadLeaseGuard {
   public:
    RpcReadLeaseGuard(MasterClient& master, std::string tenant_id,
                      std::string key, std::string lease_id)
        : master_(master),
          tenant_id_(std::move(tenant_id)),
          key_(std::move(key)),
          lease_id_(std::move(lease_id)) {}

    ~RpcReadLeaseGuard() {
        (void)master_.ReleaseVChunkReadLease(tenant_id_, key_, lease_id_);
    }

   private:
    MasterClient& master_;
    std::string tenant_id_;
    std::string key_;
    std::string lease_id_;
};

}  // namespace

tl::expected<VChunkMetadataRecord, ErrorCode>
LocalVChunkControlPlane::PutStart(const TenantId& tenant_id,
                                  const std::string& key, uint64_t total_size,
                                  int64_t now_ms) {
    return master_.VChunkPutStart(tenant_id, key, total_size, false, now_ms);
}

ErrorCode LocalVChunkControlPlane::PutEnd(const TenantId& tenant_id,
                                          const std::string& key,
                                          const std::string& vchunk_id,
                                          int64_t now_ms,
                                          uint64_t leader_epoch,
                                          const std::vector<uint64_t>&
                                              slice_checksums) {
    return master_.VChunkPutEnd(tenant_id, key, vchunk_id, now_ms,
                                leader_epoch, slice_checksums);
}

ErrorCode LocalVChunkControlPlane::PutRevoke(const TenantId& tenant_id,
                                             const std::string& key,
                                             const std::string& vchunk_id,
                                             uint64_t leader_epoch) {
    return master_.VChunkPutRevoke(tenant_id, key, vchunk_id, leader_epoch);
}

tl::expected<VChunkControlPlaneRead, ErrorCode> LocalVChunkControlPlane::Get(
    const TenantId& tenant_id, const std::string& key) {
    auto handle = master_.AcquireVChunkRead(tenant_id, key);
    if (!handle) return tl::unexpected(handle.error());
    auto lifetime = std::make_shared<VChunkMasterManager::ReadHandle>(
        std::move(*handle));
    return VChunkControlPlaneRead{lifetime->record(), std::move(lifetime)};
}

ErrorCode LocalVChunkControlPlane::Remove(const TenantId& tenant_id,
                                          const std::string& key,
                                          int64_t now_ms,
                                          uint64_t leader_epoch) {
    return master_.RemoveVChunk(tenant_id, key, now_ms, leader_epoch);
}

tl::expected<VChunkMetadataRecord, ErrorCode>
RpcVChunkControlPlane::PutStart(const TenantId& tenant_id,
                                const std::string& key, uint64_t total_size,
                                int64_t now_ms) {
    return master_.VChunkPutStart(tenant_id.value(), key, total_size, now_ms);
}

ErrorCode RpcVChunkControlPlane::PutEnd(const TenantId& tenant_id,
                                        const std::string& key,
                                        const std::string& vchunk_id,
                                        int64_t now_ms,
                                        uint64_t leader_epoch,
                                        const std::vector<uint64_t>&
                                            slice_checksums) {
    auto result = master_.VChunkPutEnd(tenant_id.value(), key, vchunk_id,
                                       now_ms, leader_epoch, slice_checksums);
    return result ? ErrorCode::OK : result.error();
}

ErrorCode RpcVChunkControlPlane::PutRevoke(const TenantId& tenant_id,
                                           const std::string& key,
                                           const std::string& vchunk_id,
                                           uint64_t leader_epoch) {
    auto result = master_.VChunkPutRevoke(tenant_id.value(), key, vchunk_id,
                                           leader_epoch);
    return result ? ErrorCode::OK : result.error();
}

tl::expected<VChunkControlPlaneRead, ErrorCode> RpcVChunkControlPlane::Get(
    const TenantId& tenant_id, const std::string& key) {
    auto record = master_.GetVChunk(tenant_id.value(), key);
    if (!record) return tl::unexpected(record.error());
    auto lifetime = std::make_shared<RpcReadLeaseGuard>(
        master_, tenant_id.value(), key, record->lease_id);
    return VChunkControlPlaneRead{std::move(record->record),
                                  std::move(lifetime)};
}

ErrorCode RpcVChunkControlPlane::Remove(const TenantId& tenant_id,
                                        const std::string& key,
                                        int64_t now_ms,
                                        uint64_t leader_epoch) {
    auto result = master_.RemoveVChunk(tenant_id.value(), key, now_ms,
                                       leader_epoch);
    return result ? ErrorCode::OK : result.error();
}

bool RefreshingVChunkControlPlane::IsRouteError(ErrorCode error) {
    return error == ErrorCode::NOT_OWNER || error == ErrorCode::ROUTE_CHANGED ||
           error == ErrorCode::STALE_EPOCH;
}

bool RefreshingVChunkControlPlane::RefreshForRetry(uint32_t attempt,
                                                   ErrorCode error) const {
    return attempt < max_route_retries_ && IsRouteError(error) && refresh_ &&
           refresh_() == ErrorCode::OK;
}

tl::expected<VChunkMetadataRecord, ErrorCode>
RefreshingVChunkControlPlane::PutStart(const TenantId& tenant_id,
                                       const std::string& key,
                                       uint64_t total_size, int64_t now_ms) {
    for (uint32_t attempt = 0;; ++attempt) {
        auto target = resolve_ ? resolve_(tenant_id, key)
                               : tl::expected<VChunkControlPlane*, ErrorCode>(
                                     tl::make_unexpected(
                                         ErrorCode::INVALID_PARAMS));
        if (!target) return tl::make_unexpected(target.error());
        auto result = (*target)->PutStart(tenant_id, key, total_size, now_ms);
        if (result || !RefreshForRetry(attempt, result.error())) return result;
    }
}

ErrorCode RefreshingVChunkControlPlane::PutEnd(
    const TenantId& tenant_id, const std::string& key,
    const std::string& vchunk_id, int64_t now_ms, uint64_t leader_epoch,
    const std::vector<uint64_t>& checksums) {
    if (!resolve_) return ErrorCode::INVALID_PARAMS;
    for (uint32_t attempt = 0;; ++attempt) {
        auto target = resolve_(tenant_id, key);
        if (!target) return target.error();
        const auto result = (*target)->PutEnd(
            tenant_id, key, vchunk_id, now_ms, leader_epoch, checksums);
        if (!RefreshForRetry(attempt, result)) return result;
    }
}

ErrorCode RefreshingVChunkControlPlane::PutRevoke(
    const TenantId& tenant_id, const std::string& key,
    const std::string& vchunk_id, uint64_t leader_epoch) {
    if (!resolve_) return ErrorCode::INVALID_PARAMS;
    for (uint32_t attempt = 0;; ++attempt) {
        auto target = resolve_(tenant_id, key);
        if (!target) return target.error();
        const auto result =
            (*target)->PutRevoke(tenant_id, key, vchunk_id, leader_epoch);
        if (!RefreshForRetry(attempt, result)) return result;
    }
}

tl::expected<VChunkControlPlaneRead, ErrorCode>
RefreshingVChunkControlPlane::Get(const TenantId& tenant_id,
                                  const std::string& key) {
    if (!resolve_) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    for (uint32_t attempt = 0;; ++attempt) {
        auto target = resolve_(tenant_id, key);
        if (!target) return tl::make_unexpected(target.error());
        auto result = (*target)->Get(tenant_id, key);
        if (result || !RefreshForRetry(attempt, result.error())) return result;
    }
}

ErrorCode RefreshingVChunkControlPlane::Remove(
    const TenantId& tenant_id, const std::string& key, int64_t now_ms,
    uint64_t leader_epoch) {
    if (!resolve_) return ErrorCode::INVALID_PARAMS;
    for (uint32_t attempt = 0;; ++attempt) {
        auto target = resolve_(tenant_id, key);
        if (!target) return target.error();
        const auto result =
            (*target)->Remove(tenant_id, key, now_ms, leader_epoch);
        if (!RefreshForRetry(attempt, result)) return result;
    }
}

void VChunkControlPlaneDirectory::SetTarget(
    std::string submaster_id, VChunkControlPlane* target) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (submaster_id.empty() || !target) {
        targets_.erase(submaster_id);
        return;
    }
    targets_[std::move(submaster_id)] = target;
}

ErrorCode VChunkControlPlaneDirectory::Refresh() {
    if (!route_store_) return ErrorCode::INVALID_PARAMS;
    auto loaded = route_store_->Load();
    if (!loaded) return loaded.error();
    VChunkDynamicRouteTable validator;
    if (validator.ApplySnapshot(*loaded) != ErrorCode::OK) {
        return ErrorCode::INVALID_VERSION;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    if (loaded->route_version < snapshot_.route_version) {
        return ErrorCode::ROUTE_CHANGED;
    }
    snapshot_ = std::move(*loaded);
    return ErrorCode::OK;
}

tl::expected<VChunkControlPlane*, ErrorCode>
VChunkControlPlaneDirectory::Resolve(const TenantId& tenant_id,
                                     const std::string& key) const {
    if (!tenant_id.IsValid() || key.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> guard(mutex_);
    if (snapshot_.slots.empty()) {
        return tl::make_unexpected(ErrorCode::ROUTE_CHANGED);
    }
    auto slot =
        ComputeVChunkSlot(tenant_id.value(), key, snapshot_.slots.size());
    if (!slot) return tl::make_unexpected(slot.error());
    const auto& route = snapshot_.slots[*slot];
    if (route.state != VChunkSlotState::OWNED) {
        return tl::make_unexpected(ErrorCode::ROUTE_CHANGED);
    }
    const auto found = targets_.find(route.owner_submaster_id);
    if (found == targets_.end() || !found->second) {
        return tl::make_unexpected(ErrorCode::NOT_OWNER);
    }
    return found->second;
}

uint64_t VChunkControlPlaneDirectory::RouteVersion() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return snapshot_.route_version;
}

}  // namespace mooncake
