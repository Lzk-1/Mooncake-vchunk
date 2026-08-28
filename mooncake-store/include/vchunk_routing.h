#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct VChunkRoute {
    uint32_t slot{0};
    std::string owner_submaster_id;
    uint64_t owner_epoch{0};
    uint64_t route_version{0};

    YLT_REFL(VChunkRoute, slot, owner_submaster_id, owner_epoch,
             route_version);
};

class VChunkStaticRouteTable {
   public:
    VChunkStaticRouteTable(uint64_t route_version,
                           std::vector<std::string> slot_owners,
                           uint64_t owner_epoch);

    ErrorCode Validate() const;
    tl::expected<VChunkRoute, ErrorCode> Resolve(
        std::string_view tenant_id, std::string_view key) const;
    ErrorCode CheckOwner(std::string_view local_submaster_id,
                         std::string_view tenant_id, std::string_view key,
                         uint64_t request_route_version,
                         uint64_t request_owner_epoch) const;

    uint64_t Version() const { return route_version_; }
    size_t SlotCount() const { return slot_owners_.size(); }

   private:
    uint64_t route_version_;
    std::vector<std::string> slot_owners_;
    uint64_t owner_epoch_;
};

}  // namespace mooncake
