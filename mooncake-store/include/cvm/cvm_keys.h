#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

namespace mooncake {
namespace cvm {

// Root prefix for all CVM keys in etcd.
inline constexpr std::string_view kCvmRootPrefix = "/cvm/";

// End key for an etcd range scan over [prefix, PrefixEnd(prefix)).
inline std::string PrefixEnd(std::string prefix) {
    for (int i = static_cast<int>(prefix.size()) - 1; i >= 0; --i) {
        unsigned char c = static_cast<unsigned char>(prefix[i]);
        if (c < 0xFF) {
            prefix[i] = static_cast<char>(c + 1);
            prefix.resize(i + 1);
            return prefix;
        }
    }
    return std::string(1, '\0');
}

// "/cvm/<namespace>/"
inline std::string CvmNamespaceRoot(const std::string& cluster_namespace) {
    return std::string(kCvmRootPrefix) + cluster_namespace + "/";
}

// "/cvm/<namespace>/kv_view/"
inline std::string KvViewPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "kv_view/";
}

// "/cvm/<namespace>/kv_view/slot/<slot:05d>"
inline std::string SlotOwnerKey(const std::string& cluster_namespace,
                                uint16_t slot) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", static_cast<unsigned>(slot));
    return KvViewPrefix(cluster_namespace) + "slot/" + buf;
}

// "/cvm/<namespace>/slot_meta/"
inline std::string SlotMetadataExportPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "slot_meta/";
}

// "/cvm/<namespace>/slot_meta/<slot:05d>"
// Binary (struct_pack) value holding the object metadata exported by the
// previous live primary owner during a slot handoff.
inline std::string SlotMetadataExportKey(const std::string& cluster_namespace,
                                         uint16_t slot) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", static_cast<unsigned>(slot));
    return SlotMetadataExportPrefix(cluster_namespace) + buf;
}

// "/cvm/<namespace>/segment_view/"
inline std::string SegmentViewPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "segment_view/";
}

// "/cvm/<namespace>/segment_view/<segment_id>"
inline std::string SegmentOwnerKey(const std::string& cluster_namespace,
                                   const std::string& segment_id) {
    return SegmentViewPrefix(cluster_namespace) + segment_id;
}

// "/cvm/<namespace>/masters/"
inline std::string MasterRegistrationPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "masters/";
}

// "/cvm/<namespace>/masters/<master_id>"
inline std::string MasterRegistrationKey(const std::string& cluster_namespace,
                                         const std::string& master_id) {
    return MasterRegistrationPrefix(cluster_namespace) + master_id;
}

// "/cvm/<namespace>/snapshot/"
inline std::string SnapshotPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "snapshot/";
}

// "/cvm/<namespace>/snapshot/kv_view"
inline std::string KvViewSnapshotKey(const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "kv_view";
}

// "/cvm/<namespace>/snapshot/segment_view"
inline std::string SegmentViewSnapshotKey(const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "segment_view";
}

// ---- New segment neutral entity + per-master mount keys (§3 view layout) ----

// "/cvm/<namespace>/segments/"
inline std::string SegmentNeutralEntityPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "segments/";
}

// "/cvm/<namespace>/segments/<segment_id>"
inline std::string SegmentNeutralEntityKey(const std::string& cluster_namespace,
                                           const std::string& segment_id) {
    return SegmentNeutralEntityPrefix(cluster_namespace) + segment_id;
}

// "/cvm/<namespace>/submaster_snapshot/"
inline std::string SubmasterSnapshotPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "submaster_snapshot/";
}

// "/cvm/<namespace>/submaster_snapshot/<master_id>/segments/"
inline std::string SubmasterSegmentsPrefix(
    const std::string& cluster_namespace, const std::string& master_id) {
    return SubmasterSnapshotPrefix(cluster_namespace) + master_id + "/segments/";
}

// "/cvm/<namespace>/submaster_snapshot/<master_id>/segments/<segment_id>"
inline std::string SubmasterSegmentMountKey(
    const std::string& cluster_namespace, const std::string& master_id,
    const std::string& segment_id) {
    return SubmasterSegmentsPrefix(cluster_namespace, master_id) + segment_id;
}

// ---- Partition 路由（§5.2 vsegment 预留接口，仅存 owner + epoch）----

// "/cvm/<namespace>/partition_route/"
inline std::string PartitionRoutePrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "partition_route/";
}

// "/cvm/<namespace>/partition_route/<partition_id>"
// 保存 Partition owner + route_epoch（迁移期另存 state 与 target）。迁移完成
// 后键仍保留，仅内容随 owner 切换更新。
inline std::string PartitionRouteKey(const std::string& cluster_namespace,
                                     const std::string& partition_id) {
    return PartitionRoutePrefix(cluster_namespace) + partition_id;
}

// "/cvm/<namespace>/segment_allocator_route/"
inline std::string PSegmentAllocatorRoutePrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "segment_allocator_route/";
}

// "/cvm/<namespace>/segment_allocator_route/<segment_id>"
// 保存 psegment 物理分配唯一写者 allocator owner + allocator_epoch。
inline std::string PSegmentAllocatorRouteKey(
    const std::string& cluster_namespace, const std::string& segment_id) {
    return PSegmentAllocatorRoutePrefix(cluster_namespace) + segment_id;
}

}  // namespace cvm
}  // namespace mooncake
