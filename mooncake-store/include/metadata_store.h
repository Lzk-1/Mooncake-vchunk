#pragma once

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "vsegment/vsegment_manager.h"
#include "types.h"
#include "partition/vsegment_types.h"

namespace mooncake {

inline std::string NormalizeTenantId(std::string_view tenant_id) {
    return TenantId(std::string(tenant_id)).value();
}

/**
 * @brief Metadata structure for Standby to store and restore object information
 *
 * This structure contains all essential metadata information needed by Standby
 * to immediately serve as Primary when promoted.
 */
struct StandbyObjectMetadata {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    // NOTE: Lease information is NOT stored because:
    // 1. Standby does not perform eviction, so lease info is not used
    // 2. After promotion, new Primary should grant fresh leases, not restore
    // old ones
    uint64_t last_sequence_id{
        0};                // Last OpLog sequence ID that modified this key
    std::string group_id;  // Tenant group identifier
    ObjectDataType data_type{
        ObjectDataType::UNKNOWN};  // Data type classification

    StandbyObjectMetadata() = default;

    // Check if this metadata has valid replicas
    bool HasReplicas() const { return !replicas.empty(); }
};
YLT_REFL(StandbyObjectMetadata, client_id, size, replicas, last_sequence_id,
         group_id, data_type);

/**
 * Segment info stored in standby's segment registry.
 * Used for recovering segment view after promotion.
 */
struct StandbySegmentInfo {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{false};
    std::string file_path;  // empty for memory segments

    YLT_REFL(StandbySegmentInfo, segment_name, transport_endpoint, capacity,
             is_memory_segment, file_path);
};

/**
 * @brief Standby object entry with tenant-aware key
 *
 * Replaces std::pair<std::string, StandbyObjectMetadata> for cleaner
 * struct_pack serialization and explicit tenant_id support.
 */
struct StandbyObjectEntry {
    std::string tenant_id{"default"};
    std::string key;
    StandbyObjectMetadata metadata;

    YLT_REFL(StandbyObjectEntry, tenant_id, key, metadata);
};

/**
 * @brief Object-metadata export for a single KV slot (live primary -> live
 * primary handoff).
 *
 * When a live primary gracefully releases a slot it no longer owns (scale-out /
 * scale-in rebalance), it exports the object metadata of every key in that
 * slot to etcd. The new owner then imports it to materialize the same
 * key -> Replica::Descriptor[] mapping without moving any data bytes (which
 * stay in their segments). Serialized with struct_pack (msgpack binary) and
 * stored as a binary etcd value.
 */
struct SlotMetadataExport {
    uint16_t slot{0};
    std::string source_master_id;
    std::vector<StandbyObjectEntry> objects;

    YLT_REFL(SlotMetadataExport, slot, source_master_id, objects);
};

/**
 * Complete snapshot exported from standby at promotion time.
 * Includes applied OpLog sequence ID, all object metadata,
 * and all registered segments.
 */
struct StandbySnapshot {
    uint64_t oplog_sequence_id{0};
    std::vector<StandbySegmentInfo> segments;
    std::vector<StandbyObjectEntry> objects;
    std::vector<vsegment::PartitionVSegmentSnapshot> vsegment_partitions;

    YLT_REFL(StandbySnapshot, oplog_sequence_id, segments, objects,
             vsegment_partitions);
};

// ---------------------------------------------------------------------------
// vsegment 持久化快照协议（§12.3.8 预留）
//
// 参照 SlotMetadataExport 的字段契约风格：仅冻结序列化字段，运行期对账与
// 恢复语义由 vsegment 实现方负责。partition / psegment allocation 的快照相互
// 独立，分别携带各自的单调递增 revision。
// ---------------------------------------------------------------------------

// 两阶段写中已预留（PutStart 已生成 operation_id）但尚未 PutEnd 提交的挂起
// 操作。恢复对账据此决定撤销（Abort）或继续提交（Commit）。
struct PendingOperation {
    std::string operation_id;
    std::string object_key;
    uint64_t route_epoch{0};
    // TODO(vsegment): 预留逻辑区间 / 关联 VSegmentAllocationState 引用等字段
    // 由 vsegment 实现方定义后补充，并同步更新 YLT_REFL。
};
YLT_REFL(PendingOperation, operation_id, object_key, route_epoch);

// Partition 级持久化快照：分区内对象元数据、不可变 view 布局、逻辑分配状态，
// 以及被挂起的两阶段写操作。metadata_revision 为分区内元数据单调递增版本。
struct PartitionSnapshot {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::vector<StandbyObjectMetadata> objects;
    std::vector<partition::VSegmentView> views;
    std::vector<partition::VSegmentAllocationState> allocation_states;
    std::vector<PendingOperation> pending_operations;
};
YLT_REFL(PartitionSnapshot, partition_id, route_epoch, metadata_revision,
         objects, views, allocation_states, pending_operations);

// psegment 物理分配持久化快照：free/reserved/committed extent 全集。
// allocation_revision 为该 allocator 的单调递增版本，独立于 Partition revision。
// TODO(vsegment): free/reserved/committed extent 的容器类型由 vsegment 实现方
// 定义后补充，并同步更新 YLT_REFL。
struct PSegmentAllocationSnapshot {
    std::string segment_id;
    uint64_t allocator_epoch{0};
    uint64_t allocation_revision{0};
};
YLT_REFL(PSegmentAllocationSnapshot, segment_id, allocator_epoch,
         allocation_revision);

/**
 * @brief Payload structure for struct_pack serialization (msgpack binary
 * format)
 *
 * Now uses UUID directly since struct_pack natively supports std::pair.
 */
struct MetadataPayload {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    struct_pack::compatible<std::string, 1> group_id;      // Tenant group
    struct_pack::compatible<ObjectDataType, 1> data_type;  // Data type

    YLT_REFL(MetadataPayload, client_id, size, replicas, group_id, data_type);

    // Convert to StandbyObjectMetadata
    StandbyObjectMetadata ToStandbyMetadata(uint64_t sequence_id) const {
        StandbyObjectMetadata meta;
        meta.client_id = client_id;
        meta.size = size;
        meta.replicas = replicas;
        meta.last_sequence_id = sequence_id;
        meta.group_id = group_id.value_or("");
        meta.data_type = data_type.value_or(ObjectDataType::UNKNOWN);
        return meta;
    }
};

/**
 * Thread-safe registry of segments known to standby.
 * Maintained by applying SEGMENT_MOUNT/UNMOUNT/UPDATE OpLog events.
 * Used to reconstruct segment view after promotion.
 */
class StandbySegmentRegistry {
   public:
    StandbySegmentRegistry() = default;

    // Segment lifecycle events
    void OnSegmentMount(const StandbySegmentInfo& info);
    void OnSegmentUnmount(const std::string& transport_endpoint);
    void OnSegmentUpdate(const StandbySegmentInfo& info);

    // Queries
    bool HasSegment(const std::string& transport_endpoint) const;
    std::optional<StandbySegmentInfo> GetSegment(
        const std::string& transport_endpoint) const;
    std::vector<StandbySegmentInfo> GetAllSegments() const;
    void Clear();

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, StandbySegmentInfo> segments_by_endpoint_;
};

/**
 * @brief Abstract interface for metadata storage on Standby
 *
 * This interface provides basic operations for storing and managing object
 * metadata. In a full implementation, this would mirror MasterService's
 * metadata_shards_ structure.
 */
class MetadataStore {
   public:
    virtual ~MetadataStore() = default;

    // NEW: tenant-aware methods (primary API)
    virtual bool PutMetadata(const std::string& tenant_id,
                             const std::string& key,
                             const StandbyObjectMetadata& metadata) = 0;
    virtual std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& tenant_id, const std::string& key) const = 0;
    virtual bool Remove(const std::string& tenant_id,
                        const std::string& key) = 0;
    virtual bool Exists(const std::string& tenant_id,
                        const std::string& key) const = 0;
    virtual size_t GetKeyCountForTenant(const std::string& tenant_id) const = 0;

    // DEPRECATED: key-only overloads delegate to tenant-aware with "default"
    virtual bool PutMetadata(const std::string& key,
                             const StandbyObjectMetadata& metadata) {
        return PutMetadata("default", key, metadata);
    }
    virtual std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string& key) const {
        return GetMetadata("default", key);
    }
    virtual bool Remove(const std::string& key) {
        return Remove("default", key);
    }
    virtual bool Exists(const std::string& key) const {
        return Exists("default", key);
    }

    // NOTE: legacy Put(key, payload) remains as default-tenant delegate.
    // StandbyMetadataStore and MockMetadataStore continue to implement it.
    virtual bool Put(const std::string& key,
                     const std::string& payload = std::string()) = 0;

    // GetKeyCount semantics unchanged - returns total across ALL tenants
    virtual size_t GetKeyCount() const = 0;
};

}  // namespace mooncake
