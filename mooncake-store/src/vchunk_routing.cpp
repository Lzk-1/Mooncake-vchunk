#include "vchunk_routing.h"

#include <limits>
#include <utility>

#include <xxhash.h>

namespace mooncake {

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
    std::string scoped(tenant_id);
    scoped.push_back('\0');
    scoped.append(key);
    const auto slot = static_cast<uint32_t>(
        XXH64(scoped.data(), scoped.size(), 0) % slot_owners_.size());
    return VChunkRoute{slot, slot_owners_[slot], owner_epoch_, route_version_};
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

}  // namespace mooncake
