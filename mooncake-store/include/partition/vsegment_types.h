#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {
namespace partition {

// ---------------------------------------------------------------------------
// 枚举（enum 字段统一以 int32_t 存储序列化，保证跨语言/编译器一致）
// ---------------------------------------------------------------------------

// vsegment 条带映射算法。当前仅支持轮询（round-robin）。
enum class MappingAlgorithm : int32_t {
    kRoundRobin = 0,
};

// vsegment 生命周期状态。
enum class Lifecycle : int32_t {
    kPreparing = 0,  // 创建中，尚未对外分配
    kActive = 1,     // 正常可分配
    kDraining = 2,   // 退役中，停止新分配、排空在途 I/O
    kRetired = 3,    // 已退役，逻辑空间与物理 extent 均已释放
};

// Partition 路由状态。
enum class PartitionState : int32_t {
    kActive = 0,     // 正常由 owner 服务
    kMigrating = 1,  // 迁移中，target_submaster_id 有效
};

// ---------------------------------------------------------------------------
// 数据模型（仅接口契约，运行期逻辑由 vsegment 实现方负责）
// ---------------------------------------------------------------------------

// 稳定 psegment 身份：跨实例生命周期不变。endpoint / 介质 / 健康状态
// 不在此冗余保存，运行时从现有 Segment Meta 查询。
struct SegmentIdentity {
    std::string segment_id;
};
YLT_REFL(SegmentIdentity, segment_id);

// 唯一归属 Partition 身份，不随 owner 迁移改变。
struct PartitionIdentity {
    std::string partition_id;
};
YLT_REFL(PartitionIdentity, partition_id);

// 成员物理区间：psegment 上的一段 [base_offset, base_offset + length)。
struct PSegmentExtent {
    SegmentIdentity segment;
    uint64_t base_offset{0};
    uint64_t length{0};
};
YLT_REFL(PSegmentExtent, segment, base_offset, length);

// 不可变逻辑布局：多个 psegment 的等长区间按条带拼成一个逻辑空间。
struct VSegmentView {
    std::string vsegment_id;
    PartitionIdentity partition_id;
    int32_t mapping_algorithm{0};  // MappingAlgorithm
    uint64_t stripe_size{0};
    std::vector<PSegmentExtent> members;  // 有序，至少 2 项，各贡献长度相等
    uint64_t logical_capacity{0};
    uint32_t checksum{0};
};
YLT_REFL(VSegmentView, vsegment_id, partition_id, mapping_algorithm,
         stripe_size, members, logical_capacity, checksum);

// 逻辑空间分配状态。committed_ranges 不在此保存，以对象元数据中的
// ReplicaDescriptor 为权威来源。
// TODO(vsegment): free_ranges / reservations 的具体区间表示（RangeSet /
// ReservationMap）由 vsegment 实现方定义后补充，并同步更新 YLT_REFL。
struct VSegmentAllocationState {
    std::string vsegment_id;
    int32_t lifecycle{0};  // Lifecycle
};
YLT_REFL(VSegmentAllocationState, vsegment_id, lifecycle);

// psegment 物理分配状态，独立于 Partition revision（不随 Partition 迁移）。
// TODO(vsegment): free_extents / reservations / committed_extents 的具体表示
// 由 vsegment 实现方定义后补充，并同步更新 YLT_REFL。
struct PSegmentAllocationState {
    std::string segment_id;
    uint64_t allocator_epoch{0};
};
YLT_REFL(PSegmentAllocationState, segment_id, allocator_epoch);

// 每个 psegment 的唯一物理分配写者路由（ETCD 仅保存 owner + epoch）。
struct PSegmentAllocatorRoute {
    std::string segment_id;
    std::string allocator_submaster_id;
    uint64_t allocator_epoch{0};
};
YLT_REFL(PSegmentAllocatorRoute, segment_id, allocator_submaster_id,
         allocator_epoch);

// Partition 路由：ETCD 保存 owner + route_epoch + state（迁移期再加 target）。
struct PartitionRoute {
    PartitionIdentity partition_id;
    std::string owner_submaster_id;
    uint64_t route_epoch{0};
    int32_t state{0};  // PartitionState
    std::string target_submaster_id;  // 迁移期有效，稳定态为空
};
YLT_REFL(PartitionRoute, partition_id, owner_submaster_id, route_epoch, state,
         target_submaster_id);

// ---------------------------------------------------------------------------
// 物理分配面 RPC 协议类型（§5.2.5，幂等接口）
// 幂等键 = (allocation_id, segment_id)。调用方重试时复用同一 allocation_id。
// ---------------------------------------------------------------------------

// allocation_id 关联 extent 的生命周期状态（QueryExtentAllocation 返回）。
enum class ExtentAllocationState : int32_t {
    kUnknown = 0,   // 无记录或已释放
    kReserved = 1,  // 已预留未提交
    kCommitted = 2, // 已提交
};

struct GetExtentSummaryRequest {
    SegmentIdentity segment;
};
YLT_REFL(GetExtentSummaryRequest, segment);

struct GetExtentSummaryResponse {
    uint64_t total_capacity{0};
    uint64_t free_capacity{0};
    uint64_t max_contiguous{0};
};
YLT_REFL(GetExtentSummaryResponse, total_capacity, free_capacity,
         max_contiguous);

struct ReserveExtentRequest {
    std::string allocation_id;
    SegmentIdentity segment;
    uint64_t allocator_epoch{0};
    std::string vsegment_id;
    PartitionIdentity partition_id;
    uint64_t requested_length{0};
    uint64_t alignment{0};
};
YLT_REFL(ReserveExtentRequest, allocation_id, segment, allocator_epoch,
         vsegment_id, partition_id, requested_length, alignment);

struct ReserveExtentResponse {
    PSegmentExtent extent;
};
YLT_REFL(ReserveExtentResponse, extent);

struct CommitExtentRequest {
    std::string allocation_id;
    SegmentIdentity segment;
    uint64_t allocator_epoch{0};
    PSegmentExtent extent;
};
YLT_REFL(CommitExtentRequest, allocation_id, segment, allocator_epoch, extent);

struct AbortExtentRequest {
    std::string allocation_id;
    SegmentIdentity segment;
    uint64_t allocator_epoch{0};
    PSegmentExtent extent;
};
YLT_REFL(AbortExtentRequest, allocation_id, segment, allocator_epoch, extent);

struct QueryExtentAllocationRequest {
    std::string allocation_id;
    SegmentIdentity segment;
};
YLT_REFL(QueryExtentAllocationRequest, allocation_id, segment);

struct QueryExtentAllocationResponse {
    int32_t state{0};  // ExtentAllocationState
    PSegmentExtent extent;
};
YLT_REFL(QueryExtentAllocationResponse, state, extent);

struct ReleaseCommittedExtentRequest {
    SegmentIdentity segment;
    uint64_t allocator_epoch{0};
    PSegmentExtent extent;
};
YLT_REFL(ReleaseCommittedExtentRequest, segment, allocator_epoch, extent);

}  // namespace partition
}  // namespace mooncake