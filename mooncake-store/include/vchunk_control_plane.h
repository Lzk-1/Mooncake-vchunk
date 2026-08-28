#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "tenant_id.h"
#include "types.h"
#include "vchunk_metadata.h"
#include "vchunk_routing.h"

namespace mooncake {

class MasterClient;
class MasterService;

struct VChunkControlPlaneRead {
    VChunkMetadataRecord record;
    // Retains either the local allocator-backed read handle or the guard for a
    // remote read lease until the data-plane transfer has finished.
    std::shared_ptr<void> lifetime;
};

class VChunkControlPlane {
   public:
    virtual ~VChunkControlPlane() = default;

    virtual tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId& tenant_id, const std::string& key, uint64_t total_size,
        int64_t now_ms) = 0;
    virtual ErrorCode PutEnd(const TenantId& tenant_id, const std::string& key,
                             const std::string& vchunk_id,
                             int64_t now_ms, uint64_t leader_epoch,
                             const std::vector<uint64_t>& slice_checksums) = 0;
    virtual ErrorCode PutRevoke(const TenantId& tenant_id,
                                const std::string& key,
                                const std::string& vchunk_id,
                                uint64_t leader_epoch) = 0;
    virtual tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId& tenant_id, const std::string& key) = 0;
    virtual ErrorCode Remove(const TenantId& tenant_id,
                             const std::string& key, int64_t now_ms,
                             uint64_t leader_epoch) = 0;
};

class LocalVChunkControlPlane final : public VChunkControlPlane {
   public:
    explicit LocalVChunkControlPlane(MasterService& master) : master_(master) {}

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId&, const std::string&, uint64_t, int64_t) override;
    ErrorCode PutEnd(const TenantId&, const std::string&, const std::string&,
                     int64_t, uint64_t,
                     const std::vector<uint64_t>&) override;
    ErrorCode PutRevoke(const TenantId&, const std::string&,
                        const std::string&, uint64_t) override;
    tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId&, const std::string&) override;
    ErrorCode Remove(const TenantId&, const std::string&, int64_t,
                     uint64_t) override;

   private:
    MasterService& master_;
};

class RpcVChunkControlPlane final : public VChunkControlPlane {
   public:
    explicit RpcVChunkControlPlane(MasterClient& master) : master_(master) {}

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId&, const std::string&, uint64_t, int64_t) override;
    ErrorCode PutEnd(const TenantId&, const std::string&, const std::string&,
                     int64_t, uint64_t,
                     const std::vector<uint64_t>&) override;
    ErrorCode PutRevoke(const TenantId&, const std::string&,
                        const std::string&, uint64_t) override;
    tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId&, const std::string&) override;
    ErrorCode Remove(const TenantId&, const std::string&, int64_t,
                     uint64_t) override;

   private:
    MasterClient& master_;
};

class RefreshingVChunkControlPlane final : public VChunkControlPlane {
   public:
    using ResolveFn = std::function<tl::expected<VChunkControlPlane*, ErrorCode>(
        const TenantId&, const std::string&)>;
    using RefreshFn = std::function<ErrorCode()>;

    RefreshingVChunkControlPlane(ResolveFn resolve, RefreshFn refresh,
                                 uint32_t max_route_retries = 2)
        : resolve_(std::move(resolve)),
          refresh_(std::move(refresh)),
          max_route_retries_(max_route_retries) {}

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId&, const std::string&, uint64_t, int64_t) override;
    ErrorCode PutEnd(const TenantId&, const std::string&, const std::string&,
                     int64_t, uint64_t,
                     const std::vector<uint64_t>&) override;
    ErrorCode PutRevoke(const TenantId&, const std::string&,
                        const std::string&, uint64_t) override;
    tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId&, const std::string&) override;
    ErrorCode Remove(const TenantId&, const std::string&, int64_t,
                     uint64_t) override;

   private:
    static bool IsRouteError(ErrorCode error);
    bool RefreshForRetry(uint32_t attempt, ErrorCode error) const;

    ResolveFn resolve_;
    RefreshFn refresh_;
    uint32_t max_route_retries_;
};

class VChunkControlPlaneDirectory {
   public:
    explicit VChunkControlPlaneDirectory(
        std::shared_ptr<VChunkRouteStore> route_store)
        : route_store_(std::move(route_store)) {}

    void SetTarget(std::string submaster_id, VChunkControlPlane* target);
    ErrorCode Refresh();
    tl::expected<VChunkControlPlane*, ErrorCode> Resolve(
        const TenantId& tenant_id, const std::string& key) const;
    uint64_t RouteVersion() const;

   private:
    std::shared_ptr<VChunkRouteStore> route_store_;
    mutable std::mutex mutex_;
    VChunkRouteSnapshot snapshot_;
    std::unordered_map<std::string, VChunkControlPlane*> targets_;
};

}  // namespace mooncake
