#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "types.h"
#include "vchunk_config.h"

namespace mooncake {

inline constexpr uint32_t kVChunkMetadataSchemaVersion = 3;

enum class VCSliceStatus : uint8_t {
    PENDING = 0,
    COMPLETED = 1,
    FAILED = 2,
};

enum class VChunkStatus : uint8_t {
    CREATING = 0,
    ACTIVE = 1,
    RELEASING = 2,
    RELEASED = 3,
    FAILED = 4,
    RECOVERING = 5,
    ABANDONED = 6,
};

struct VCSliceDescriptor {
    uint32_t slice_index{0};
    std::string target_segment_name;
    uint64_t target_offset{0};
    uint32_t logical_length{0};
    uint32_t allocated_length{0};
    VCSliceStatus status{VCSliceStatus::PENDING};
    uint32_t retry_count{0};
    uint32_t replica_group_id{0};
    uint8_t replica_index{0};
    std::string segment_instance_id;
    uint64_t allocation_generation{0};
    uint64_t content_checksum{0};

    YLT_REFL(VCSliceDescriptor, slice_index, target_segment_name,
             target_offset, logical_length, allocated_length, status,
             retry_count, replica_group_id, replica_index,
             segment_instance_id, allocation_generation, content_checksum);
};

struct SliceGroup {
    uint32_t group_id{0};
    std::string segment_name;
    uint8_t replica_index{0};
    std::vector<uint32_t> slice_indices;

    YLT_REFL(SliceGroup, group_id, segment_name, replica_index, slice_indices);
};

struct VChunkMetadataIndex {
    uint32_t schema_version{kVChunkMetadataSchemaVersion};
    std::string vchunk_id;
    std::string tenant_id;
    std::string key;
    uint64_t total_size{0};
    uint32_t slice_count{0};
    VCSliceSizeLevel slice_size_level{VCSliceSizeLevel::k4K};
    uint32_t row_size{0};
    VChunkStatus status{VChunkStatus::CREATING};
    int64_t created_at_ms{0};
    int64_t last_updated_at_ms{0};
    uint8_t replica_num{1};
    uint64_t leader_epoch{0};
    uint64_t metadata_version{0};
    uint32_t owner_slot{0};
    std::string owner_submaster_id;
    uint64_t owner_epoch{0};
    uint64_t route_version{0};
    std::vector<SliceGroup> slice_groups;

    YLT_REFL(VChunkMetadataIndex, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, row_size, status,
             created_at_ms, last_updated_at_ms, replica_num, leader_epoch,
             metadata_version, owner_slot, owner_submaster_id, owner_epoch,
             route_version, slice_groups);
};

struct VCSlicePartition {
    std::string segment_name;
    std::vector<VCSliceDescriptor> slices;

    YLT_REFL(VCSlicePartition, segment_name, slices);
};

// Stable wire/storage representation. Runtime-only synchronization and indexes
// intentionally live outside this type.
struct VChunkMetadataRecord {
    uint32_t schema_version{kVChunkMetadataSchemaVersion};
    std::string vchunk_id;
    std::string tenant_id;
    std::string key;
    uint64_t total_size{0};
    uint32_t slice_count{0};
    VCSliceSizeLevel slice_size_level{VCSliceSizeLevel::k4K};
    std::vector<VCSliceDescriptor> slices;
    uint32_t row_size{0};
    VChunkStatus status{VChunkStatus::CREATING};
    int64_t created_at_ms{0};
    int64_t last_updated_at_ms{0};
    uint8_t replica_num{1};
    uint64_t leader_epoch{0};
    uint64_t metadata_version{0};
    uint32_t owner_slot{0};
    std::string owner_submaster_id;
    uint64_t owner_epoch{0};
    uint64_t route_version{0};
    std::vector<SliceGroup> slice_groups;

    YLT_REFL(VChunkMetadataRecord, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, slices, row_size,
             status, created_at_ms, last_updated_at_ms, replica_num,
             leader_epoch, metadata_version, owner_slot, owner_submaster_id,
             owner_epoch, route_version, slice_groups);
};

struct VChunkRuntimeInfo {
    bool enabled{false};
    bool persistent_metadata{false};

    YLT_REFL(VChunkRuntimeInfo, enabled, persistent_metadata);
};

struct VChunkReadLease {
    VChunkMetadataRecord record;
    std::string lease_id;

    YLT_REFL(VChunkReadLease, record, lease_id);
};

ErrorCode ValidateVChunkMetadata(const VChunkMetadataRecord& record,
                                 const VChunkConfig& config);
ErrorCode ValidateVChunkTransition(VChunkStatus from, VChunkStatus to);

VChunkMetadataIndex BuildVChunkMetadataIndex(
    const VChunkMetadataRecord& record);
std::vector<VCSlicePartition> PartitionVChunkSlices(
    const VChunkMetadataRecord& record);
tl::expected<std::vector<char>, ErrorCode> SerializeVChunkMetadataIndex(
    const VChunkMetadataIndex& index, const VChunkConfig& config);
tl::expected<VChunkMetadataIndex, ErrorCode> DeserializeVChunkMetadataIndex(
    const std::vector<char>& bytes, const VChunkConfig& config);
tl::expected<std::vector<char>, ErrorCode> SerializeVChunkSlicePartition(
    const VCSlicePartition& partition, const VChunkConfig& config);
tl::expected<VCSlicePartition, ErrorCode> DeserializeVChunkSlicePartition(
    const std::vector<char>& bytes, const VChunkConfig& config);
tl::expected<VChunkMetadataRecord, ErrorCode> AssembleVChunkMetadata(
    VChunkMetadataIndex index, std::vector<VCSlicePartition> partitions,
    const VChunkConfig& config);

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkMetadata(
    const VChunkMetadataRecord& record, const VChunkConfig& config);
tl::expected<VChunkMetadataRecord, ErrorCode> DeserializeVChunkMetadata(
    const std::vector<char>& bytes, const VChunkConfig& config);

class VChunkMetadata {
   public:
    explicit VChunkMetadata(VChunkMetadataRecord record);

    VChunkMetadata(const VChunkMetadata&) = delete;
    VChunkMetadata& operator=(const VChunkMetadata&) = delete;

    VChunkMetadataRecord Snapshot() const;
    ErrorCode TransitionTo(VChunkStatus next, int64_t updated_at_ms);

   private:
    mutable SpinLock lock_;
    VChunkMetadataRecord record_;
};

}  // namespace mooncake
