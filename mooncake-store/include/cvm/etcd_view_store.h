#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "cvm/cvm_types.h"
#include "partition/vsegment_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Serialization + etcd persistence for CVM views, built on EtcdHelper.
//
// All KV view / segment view / master registration records are stored in etcd
// under the CVM key space (see cvm_keys.h). ViewVersionId is the etcd revision
// returned by the read, so consumers can watch from it without gaps.
class EtcdViewStore {
   public:
    // ---- JSON serialization ----
    static ErrorCode SerializeSlotOwner(const SlotOwner& owner,
                                        std::string& out);
    static ErrorCode DeserializeSlotOwner(const std::string& in,
                                          SlotOwner& out);

    static ErrorCode SerializeSegmentOwner(const SegmentOwner& owner,
                                           std::string& out);
    static ErrorCode DeserializeSegmentOwner(const std::string& in,
                                             SegmentOwner& out);

    static ErrorCode SerializeMasterRegistration(const MasterRegistration& reg,
                                                 std::string& out);
    static ErrorCode DeserializeMasterRegistration(const std::string& in,
                                                   MasterRegistration& out);

    static ErrorCode SerializeKvViewSnapshot(const KvViewSnapshot& snapshot,
                                             std::string& out);
    static ErrorCode DeserializeKvViewSnapshot(const std::string& in,
                                               KvViewSnapshot& out);
    static ErrorCode SerializeSegmentViewSnapshot(
        const SegmentViewSnapshot& snapshot, std::string& out);
    static ErrorCode DeserializeSegmentViewSnapshot(const std::string& in,
                                                    SegmentViewSnapshot& out);

    // ---- KV view ----
    static ErrorCode LoadSlotOwner(const std::string& cluster_namespace,
                                   uint16_t slot, SlotOwner& out,
                                   ViewVersionId& version);
    static ErrorCode SaveSlotOwner(const std::string& cluster_namespace,
                                   const SlotOwner& owner);
    static ErrorCode SaveSlotOwnerWithLease(const std::string& cluster_namespace,
                                            const SlotOwner& owner,
                                            EtcdLeaseId lease_id);
    // Batch variant: writes many slot owners in chunks (each chunk a single
    // etcd txn, all bound to lease_id). Speeds up bulk acquisition (e.g. cold
    // start / node add) that would otherwise issue one RPC per slot. Falls back
    // to per-slot puts within a chunk if the txn fails; returns an error only
    // if a final per-slot put fails.
    static ErrorCode SaveSlotOwnersWithLease(
        const std::string& cluster_namespace,
        const std::vector<SlotOwner>& owners, EtcdLeaseId lease_id);
    static ErrorCode DeleteSlotOwner(const std::string& cluster_namespace,
                                     uint16_t slot);
    static ErrorCode DeleteSlotOwnerIfOwnedBy(
        const std::string& cluster_namespace, uint16_t slot,
        const std::string& master_id);
    static ErrorCode LoadAllSlotOwners(const std::string& cluster_namespace,
                                       std::vector<SlotOwner>& out,
                                       ViewVersionId& version);

    // ---- Segment view (reserved, deprecated in favor of per-master mount) ----
    static ErrorCode LoadSegmentOwner(const std::string& cluster_namespace,
                                      const std::string& segment_id,
                                      SegmentOwner& out,
                                      ViewVersionId& version);
    static ErrorCode SaveSegmentOwner(const std::string& cluster_namespace,
                                      const SegmentOwner& owner);
    static ErrorCode SaveSegmentOwnerWithLease(
        const std::string& cluster_namespace, const SegmentOwner& owner,
        EtcdLeaseId lease_id);
    static ErrorCode DeleteSegmentOwner(const std::string& cluster_namespace,
                                        const std::string& segment_id);
    static ErrorCode LoadAllSegmentOwners(const std::string& cluster_namespace,
                                          std::vector<SegmentOwner>& out,
                                          ViewVersionId& version);

    // ---- Segment neutral entity (segments/{segment_id}) ----
    static ErrorCode SerializeSegmentDescriptor(const SegmentDescriptor& desc,
                                                std::string& out);
    static ErrorCode DeserializeSegmentDescriptor(const std::string& in,
                                                  SegmentDescriptor& out);
    static ErrorCode SaveSegmentDescriptor(const std::string& cluster_namespace,
                                           const SegmentDescriptor& desc);
    static ErrorCode LoadSegmentDescriptor(const std::string& cluster_namespace,
                                           const std::string& segment_id,
                                           SegmentDescriptor& out,
                                           ViewVersionId& version);
    static ErrorCode DeleteSegmentDescriptor(
        const std::string& cluster_namespace, const std::string& segment_id);

    // ---- Per-master segment mount (submaster_snapshot/{id}/segments/{seg}) ----
    static ErrorCode SerializeMountEntry(const MountEntry& entry,
                                         std::string& out);
    static ErrorCode DeserializeMountEntry(const std::string& in,
                                           MountEntry& out);
    static ErrorCode SaveMountEntryWithLease(const std::string& cluster_namespace,
                                             const std::string& master_id,
                                             const MountEntry& entry,
                                             EtcdLeaseId lease_id);
    static ErrorCode DeleteMountEntry(const std::string& cluster_namespace,
                                      const std::string& master_id,
                                      const std::string& segment_id);
    static ErrorCode LoadSubmasterSegmentMounts(
        const std::string& cluster_namespace, const std::string& master_id,
        std::vector<MountEntry>& out, ViewVersionId& version);

    // ---- Master registration ----
    static ErrorCode RegisterMaster(const std::string& cluster_namespace,
                                    const MasterRegistration& reg,
                                    EtcdLeaseId lease_id);
    static ErrorCode UpdateMasterRole(const std::string& cluster_namespace,
                                      const std::string& master_id,
                                      MasterRole role, EtcdLeaseId lease_id);
    static ErrorCode LoadAllMasters(const std::string& cluster_namespace,
                                    std::vector<MasterRegistration>& out,
                                    ViewVersionId& version);

    // ---- Snapshots ----
    // Reads the raw slot/segment records, aggregates them into a point-in-time
    // snapshot and writes it back to etcd. `version` is set to the etcd
    // revision of the raw records that were aggregated.
    static ErrorCode BuildAndSaveKvViewSnapshot(
        const std::string& cluster_namespace, ViewVersionId& version);
    static ErrorCode BuildAndSaveSegmentViewSnapshot(
        const std::string& cluster_namespace, ViewVersionId& version);

    static ErrorCode SaveKvViewSnapshot(const std::string& cluster_namespace,
                                        const KvViewSnapshot& snapshot);
    static ErrorCode SaveSegmentViewSnapshot(
        const std::string& cluster_namespace, const SegmentViewSnapshot& snapshot);

    // ---- Partition 路由（§5.2 vsegment 预留接口，仅存 owner + epoch）----
    // 注意：ETCD 只保存路由（owner + epoch），不保存 free extents / 完整 view。
    // 方法体由 vsegment 实现方落地，此处仅冻结方法签名。
    static ErrorCode SerializePartitionRoute(
        const partition::PartitionRoute& route, std::string& out);
    static ErrorCode DeserializePartitionRoute(const std::string& in,
                                               partition::PartitionRoute& out);
    static ErrorCode SerializePSegmentAllocatorRoute(
        const partition::PSegmentAllocatorRoute& route, std::string& out);
    static ErrorCode DeserializePSegmentAllocatorRoute(
        const std::string& in, partition::PSegmentAllocatorRoute& out);

    static ErrorCode SavePartitionRoute(const std::string& cluster_namespace,
                                        const partition::PartitionRoute& route);
    static ErrorCode LoadPartitionRoute(const std::string& cluster_namespace,
                                        const std::string& partition_id,
                                        partition::PartitionRoute& out,
                                        ViewVersionId& version);
    static ErrorCode SavePSegmentAllocatorRoute(
        const std::string& cluster_namespace,
        const partition::PSegmentAllocatorRoute& route);
    static ErrorCode LoadPSegmentAllocatorRoute(
        const std::string& cluster_namespace, const std::string& segment_id,
        partition::PSegmentAllocatorRoute& out, ViewVersionId& version);

    // 原子切换 owner：仅当 etcd 中当前 epoch 与 expected 一致时成功，成功后
    // epoch = expected + 1，并把新路由写入 out。失配返回 STALE_ROUTE /
    // STALE_ALLOCATOR_EPOCH（对应 §5.2.9 的 fencing 语义）。
    static ErrorCode CASSwitchPartitionOwner(
        const std::string& cluster_namespace, const std::string& partition_id,
        uint64_t expected_route_epoch, const std::string& new_owner_submaster_id,
        partition::PartitionState new_state,
        const std::string& target_submaster_id, partition::PartitionRoute& out);
    static ErrorCode CASSwitchPSegmentAllocator(
        const std::string& cluster_namespace, const std::string& segment_id,
        uint64_t expected_allocator_epoch,
        const std::string& new_allocator_submaster_id,
        partition::PSegmentAllocatorRoute& out);

    // ---- Watch ----
    using WatchCallback = void (*)(void*, const char*, size_t, const char*,
                                   size_t, int, int64_t);
    static ErrorCode WatchKvView(const std::string& cluster_namespace,
                                 ViewVersionId start_revision, void* ctx,
                                 WatchCallback cb);
    static ErrorCode CancelWatchKvView(const std::string& cluster_namespace);
    static ErrorCode WaitWatchKvViewStopped(const std::string& cluster_namespace,
                                            int timeout_ms);

    // ---- Master membership watch ----
    // Watches the master registration prefix so member add/remove (e.g. lease
    // expiry) can drive immediate role re-evaluation (P3 failover).
    static ErrorCode WatchMasters(const std::string& cluster_namespace,
                                  ViewVersionId start_revision, void* ctx,
                                  WatchCallback cb);
    static ErrorCode CancelWatchMasters(const std::string& cluster_namespace);
    static ErrorCode WaitWatchMastersStopped(
        const std::string& cluster_namespace, int timeout_ms);
};

}  // namespace cvm
}  // namespace mooncake
