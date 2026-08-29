#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "tenant_id.h"
#include "vchunk_allocation_strategy.h"
#include "vchunk_config.h"
#include "vchunk_ha_codec.h"
#include "vchunk_metadata.h"
#include "vchunk_metadata_store.h"
#include "vchunk_metrics.h"
#include "vchunk_recovery.h"
#include "vchunk_routing.h"

namespace mooncake {

struct VChunkScrubReport {
    uint64_t runtime_records{0};
    uint64_t persistent_records{0};
    uint64_t invalid_records{0};
    uint64_t missing_persistent_records{0};
    uint64_t stale_persistent_records{0};
    uint64_t unexpected_persistent_records{0};
    uint64_t ownership_mismatches{0};
    uint64_t overlapping_ranges{0};

    bool clean() const {
        return invalid_records == 0 && missing_persistent_records == 0 &&
               stale_persistent_records == 0 &&
               unexpected_persistent_records == 0 &&
               ownership_mismatches == 0 && overlapping_ranges == 0;
    }

    YLT_REFL(VChunkScrubReport, runtime_records, persistent_records,
             invalid_records, missing_persistent_records,
             stale_persistent_records, unexpected_persistent_records,
             ownership_mismatches, overlapping_ranges);
};

// In-memory master-side vchunk lifecycle manager. The caller must hold the
// SegmentManager allocator access guard while PutStart uses AllocatorManager.
class VChunkMasterManager {
   public:
    using DurabilitySink = std::function<ErrorCode(
        VChunkHAEventType, const VChunkMetadataRecord&)>;
    using OwnershipPredicate =
        std::function<bool(const VChunkMetadataRecord&)>;
    class ReadHandle {
       public:
        ReadHandle() = default;
        const VChunkMetadataRecord& record() const { return record_; }

       private:
        friend class VChunkMasterManager;
        VChunkMetadataRecord record_;
        std::shared_ptr<const void> lifetime_;
    };

    explicit VChunkMasterManager(
        VChunkConfig config,
        std::shared_ptr<VChunkMetadataStore> metadata_store = nullptr,
        std::shared_ptr<VChunkMetrics> metrics = nullptr,
        std::shared_ptr<VChunkRouteStore> route_store = nullptr);

    VChunkMasterManager(const VChunkMasterManager&) = delete;
    VChunkMasterManager& operator=(const VChunkMasterManager&) = delete;

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const AllocatorManager& allocator_manager, const TenantId& tenant_id,
        const std::string& key, uint64_t total_size, bool is_ssd_segment,
        int64_t now_ms,
        const std::set<std::string>& excluded_segments = {},
        uint64_t expected_leader_epoch = 0);

    ErrorCode PutEnd(const TenantId& tenant_id, const std::string& key,
                     const std::string& vchunk_id, int64_t now_ms,
                     uint64_t expected_leader_epoch = 0,
                     const std::vector<uint64_t>& slice_checksums = {});
    ErrorCode PutRevoke(const TenantId& tenant_id, const std::string& key,
                        const std::string& vchunk_id,
                        uint64_t expected_leader_epoch = 0);

    tl::expected<VChunkMetadataRecord, ErrorCode> Get(
        const TenantId& tenant_id, const std::string& key) const;
    tl::expected<ReadHandle, ErrorCode> AcquireRead(
        const TenantId& tenant_id, const std::string& key) const;

    ErrorCode Remove(const TenantId& tenant_id, const std::string& key,
                     int64_t now_ms, uint64_t expected_leader_epoch = 0);

    ErrorCode ActivateLeaderEpoch(uint64_t leader_epoch);
    void DeactivateLeader();
    uint64_t LeaderEpoch() const { return leader_epoch_.load(); }
    bool AcceptsMutations() const { return accepts_mutations_.load(); }
    ErrorCode PublishRecoveryView(VChunkRecoveryView view);
    ErrorCode ApplyRouteSnapshot(VChunkRouteSnapshot snapshot);
    ErrorCode BeginSlotTransfer(uint32_t slot, std::string target_submaster_id,
                                uint64_t next_route_version);
    ErrorCode MarkSlotTransferring(uint32_t slot,
                                   uint64_t next_route_version);
    ErrorCode CompleteSlotTransfer(uint32_t slot, uint64_t next_owner_epoch,
                                   uint64_t next_route_version);
    ErrorCode AbortSlotTransfer(uint32_t slot, uint64_t next_route_version);
    void SetDurabilitySink(DurabilitySink sink);
    void SetMembershipCheck(std::function<bool()> check);

    ErrorCode Recover(int64_t now_ms, OwnershipPredicate owns = {});
    tl::expected<size_t, ErrorCode> ReapExpired(int64_t now_ms,
                                                size_t max_scan,
                                                OwnershipPredicate owns = {});
    VChunkMetricsSnapshot MetricsSnapshot() const;
    tl::expected<VChunkScrubReport, ErrorCode> Scrub() const;

    size_t SizeForTesting() const;
    bool HasPersistentMetadata() const { return metadata_store_->IsPersistent(); }

   private:
    struct Entry {
        VChunkMetadataRecord record;
        std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
    };

    static std::string ScopedKey(const TenantId& tenant_id,
                                 const std::string& key);
    void RefreshStateMetricsLocked();
    void ReleasePendingPut(const std::string& scoped_key);
    ErrorCode CheckLeaderEpoch(uint64_t expected_leader_epoch) const;
    ErrorCode CheckStaticOwner(const TenantId& tenant_id,
                               const std::string& key) const;
    tl::expected<VChunkRoute, ErrorCode> ResolveRoute(
        const TenantId& tenant_id, const std::string& key) const;
    ErrorCode PersistEvent(VChunkHAEventType type,
                           const VChunkMetadataRecord& record) const;
    bool SlotHasEntries(uint32_t slot) const;
    ErrorCode PublishRouteSnapshot(VChunkRouteSnapshot snapshot);

    const VChunkConfig config_;
    const std::shared_ptr<VChunkMetadataStore> metadata_store_;
    const std::shared_ptr<VChunkMetrics> metrics_;
    const std::shared_ptr<VChunkRouteStore> route_store_;
    std::atomic<uint64_t> leader_epoch_{1};
    std::atomic<bool> accepts_mutations_{true};
    std::optional<VChunkStaticRouteTable> static_routes_;
    VChunkDynamicRouteTable dynamic_routes_;
    DurabilitySink durability_sink_;
    std::function<bool()> membership_check_;
    mutable std::mutex route_mutex_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
    std::unordered_set<std::string> pending_puts_;
    std::string reaper_cursor_key_;
};

}  // namespace mooncake
