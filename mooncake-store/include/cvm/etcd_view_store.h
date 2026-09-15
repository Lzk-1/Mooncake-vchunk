#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "cvm/cvm_types.h"
#include "partition/vsegment_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Serialization + etcd persistence for CVM metadata, built on EtcdHelper.
//
// Master registration, ring metadata, segment neutral descriptors and
// per-master segment mounts are stored in etcd under the CVM key space (see
// cvm_keys.h). ViewVersionId is the etcd revision returned by reads, so
// consumers can watch from it without gaps.
class EtcdViewStore {
   public:
    // ---- JSON serialization ----
    static ErrorCode SerializeMasterRegistration(const MasterRegistration& reg,
                                                 std::string& out);
    static ErrorCode DeserializeMasterRegistration(const std::string& in,
                                                   MasterRegistration& out);

    static ErrorCode SerializeRingMeta(const RingMeta& meta, std::string& out);
    static ErrorCode DeserializeRingMeta(const std::string& in, RingMeta& out);

    // ---- Segment neutral entity (segments/{segment_id}) ----
    static ErrorCode SerializeSegmentDescriptor(const SegmentDescriptor& desc,
                                                std::string& out);
    static ErrorCode DeserializeSegmentDescriptor(const std::string& in,
                                                  SegmentDescriptor& out);
    static ErrorCode SaveSegmentDescriptor(const std::string& cluster_namespace,
                                           const SegmentDescriptor& desc);
    static ErrorCode DeleteSegmentDescriptor(
        const std::string& cluster_namespace, const std::string& segment_id);
    static ErrorCode LoadAllSegmentDescriptors(
        const std::string& cluster_namespace,
        std::vector<SegmentDescriptor>& out, ViewVersionId& version);

    // ---- Per-master segment mount (snapshot/{id}/segments/{seg}) ----
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
    // Aggregates every mount record under snapshot/*/segments/, returning
    // (master_id, MountEntry) pairs so consumers can map each mount back to
    // its owning master.
    static ErrorCode LoadAllMountEntries(
        const std::string& cluster_namespace,
        std::vector<std::pair<std::string, MountEntry>>& out,
        ViewVersionId& version);

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

    // ---- Cluster ring metadata (cluster_meta) ----
    // Persists/reads the cluster-wide RingMeta { submaster_count } (§15.3).
    // Masters write it once at startup (idempotent); clients read it to derive
    // the same primary ring locally.
    static ErrorCode SaveClusterMeta(const std::string& cluster_namespace,
                                     const RingMeta& meta);
    static ErrorCode LoadClusterMeta(const std::string& cluster_namespace,
                                     RingMeta& out, ViewVersionId& version);

    // ---- Partition 路由（§5.2 vsegment 预留接口，仅存 owner + epoch）----
    // 注意：ETCD 只保存路由（owner + epoch），不保存 free extents / 完整 view。
    // 方法体由 vsegment 实现方落地，此处仅冻结方法签名。
    static ErrorCode SerializePartitionRoute(
        const partition::PartitionRoute& route, std::string& out);
    static ErrorCode DeserializePartitionRoute(const std::string& in,
                                               partition::PartitionRoute& out);

    static ErrorCode SavePartitionRoute(const std::string& cluster_namespace,
                                        const partition::PartitionRoute& route);
    static ErrorCode LoadPartitionRoute(const std::string& cluster_namespace,
                                        const std::string& partition_id,
                                        partition::PartitionRoute& out,
                                        ViewVersionId& version);

    // 原子切换 owner：仅当 etcd 中当前 epoch 与 expected 一致时成功，成功后
    // epoch = expected + 1，并把新路由写入 out。失配返回 STALE_ROUTE /
    // STALE_ALLOCATOR_EPOCH（对应 §5.2.9 的 fencing 语义）。
    static ErrorCode CASSwitchPartitionOwner(
        const std::string& cluster_namespace, const std::string& partition_id,
        uint64_t expected_route_epoch, const std::string& new_owner_submaster_id,
        partition::PartitionState new_state,
        const std::string& target_submaster_id, partition::PartitionRoute& out);

    // ---- Watch ----
    using WatchCallback = void (*)(void*, const char*, size_t, const char*,
                                   size_t, int, int64_t);

    // ---- Master membership watch ----
    // Watches the master registration prefix so member add/remove (e.g. lease
    // expiry) can drive immediate role re-evaluation.
    static ErrorCode WatchMasters(const std::string& cluster_namespace,
                                  ViewVersionId start_revision, void* ctx,
                                  WatchCallback cb);
    static ErrorCode CancelWatchMasters(const std::string& cluster_namespace);
    static ErrorCode WaitWatchMastersStopped(
        const std::string& cluster_namespace, int timeout_ms);
};

}  // namespace cvm
}  // namespace mooncake