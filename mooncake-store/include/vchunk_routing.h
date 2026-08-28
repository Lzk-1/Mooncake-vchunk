#pragma once

#include <cstdint>
#include <mutex>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

tl::expected<uint32_t, ErrorCode> ComputeVChunkSlot(
    std::string_view tenant_id, std::string_view key, size_t slot_count);

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

enum class VChunkSlotState : uint8_t {
    OWNED = 0,
    DRAINING = 1,
    TRANSFERRING = 2,
};

struct VChunkSlotRoute {
    uint32_t slot{0};
    VChunkSlotState state{VChunkSlotState::OWNED};
    std::string owner_submaster_id;
    std::string target_submaster_id;
    uint64_t owner_epoch{0};

    YLT_REFL(VChunkSlotRoute, slot, state, owner_submaster_id,
             target_submaster_id, owner_epoch);
};

struct VChunkRouteSnapshot {
    uint64_t route_version{0};
    std::vector<VChunkSlotRoute> slots;

    YLT_REFL(VChunkRouteSnapshot, route_version, slots);
};

class VChunkDynamicRouteTable {
   public:
    ErrorCode ApplySnapshot(VChunkRouteSnapshot snapshot);
    ErrorCode BeginTransfer(uint32_t slot, std::string target_submaster_id,
                            uint64_t next_route_version);
    ErrorCode MarkTransferring(uint32_t slot, uint64_t next_route_version);
    ErrorCode CompleteTransfer(uint32_t slot, uint64_t next_owner_epoch,
                               uint64_t next_route_version);
    tl::expected<VChunkSlotRoute, ErrorCode> Resolve(uint32_t slot) const;
    uint64_t Version() const;

   private:
    static ErrorCode ValidateSnapshot(const VChunkRouteSnapshot& snapshot);

    mutable std::mutex mutex_;
    VChunkRouteSnapshot snapshot_;
};

class VChunkRouteStore {
   public:
    virtual ~VChunkRouteStore() = default;
    virtual tl::expected<VChunkRouteSnapshot, ErrorCode> Load() = 0;
    virtual ErrorCode Publish(uint64_t expected_version,
                              const VChunkRouteSnapshot& snapshot) = 0;
    virtual bool IsPersistent() const = 0;
};

class InMemoryVChunkRouteStore final : public VChunkRouteStore {
   public:
    tl::expected<VChunkRouteSnapshot, ErrorCode> Load() override;
    ErrorCode Publish(uint64_t expected_version,
                      const VChunkRouteSnapshot& snapshot) override;
    bool IsPersistent() const override { return false; }

   private:
    std::mutex mutex_;
    std::optional<VChunkRouteSnapshot> snapshot_;
};

class EtcdVChunkRouteStore final : public VChunkRouteStore {
   public:
    EtcdVChunkRouteStore(std::string endpoints, std::string cluster_id);
    tl::expected<VChunkRouteSnapshot, ErrorCode> Load() override;
    ErrorCode Publish(uint64_t expected_version,
                      const VChunkRouteSnapshot& snapshot) override;
    bool IsPersistent() const override { return true; }
    ErrorCode connection_error() const { return connection_error_; }

   private:
    std::string route_key_;
    ErrorCode connection_error_{ErrorCode::OK};
};

}  // namespace mooncake
