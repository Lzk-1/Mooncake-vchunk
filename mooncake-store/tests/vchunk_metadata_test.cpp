#include "vchunk_metadata.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace mooncake {
namespace {

struct LegacySlice {
    uint32_t slice_index{0};
    std::string target_segment_name;
    uint64_t target_offset{0};
    uint32_t logical_length{0};
    uint32_t allocated_length{0};
    VCSliceStatus status{VCSliceStatus::PENDING};
    uint32_t retry_count{0};
    YLT_REFL(LegacySlice, slice_index, target_segment_name, target_offset,
             logical_length, allocated_length, status, retry_count);
};

struct LegacyRecord {
    uint32_t schema_version{1};
    std::string vchunk_id;
    std::string tenant_id;
    std::string key;
    uint64_t total_size{0};
    uint32_t slice_count{0};
    VCSliceSizeLevel slice_size_level{VCSliceSizeLevel::k4K};
    std::vector<LegacySlice> slices;
    uint32_t row_size{0};
    VChunkStatus status{VChunkStatus::CREATING};
    int64_t created_at_ms{0};
    int64_t last_updated_at_ms{0};
    YLT_REFL(LegacyRecord, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, slices, row_size,
             status, created_at_ms, last_updated_at_ms);
};

struct LegacyRecordV2 {
    uint32_t schema_version{2};
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
    std::vector<SliceGroup> slice_groups;
    YLT_REFL(LegacyRecordV2, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, slices, row_size,
             status, created_at_ms, last_updated_at_ms, replica_num,
             leader_epoch, metadata_version, slice_groups);
};

struct LegacyIndexV2 {
    uint32_t schema_version{2};
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
    std::vector<SliceGroup> slice_groups;
    YLT_REFL(LegacyIndexV2, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, row_size, status,
             created_at_ms, last_updated_at_ms, replica_num, leader_epoch,
             metadata_version, slice_groups);
};

VChunkMetadataRecord MakeValidRecord() {
    VChunkMetadataRecord record;
    record.vchunk_id = "vchunk-1";
    record.tenant_id = "tenant-1";
    record.key = "key-1";
    record.total_size = 10U * 1024U;
    record.slice_count = 3;
    record.slice_size_level = VCSliceSizeLevel::k4K;
    record.row_size = 2;
    record.status = VChunkStatus::CREATING;
    record.created_at_ms = 100;
    record.last_updated_at_ms = 100;
    record.slices = {
        VCSliceDescriptor{0, "segment-a", 0, 4096, 4096,
                          VCSliceStatus::PENDING, 0},
        VCSliceDescriptor{1, "segment-b", 0, 4096, 4096,
                          VCSliceStatus::PENDING, 0},
        VCSliceDescriptor{2, "segment-a", 4096, 2048, 4096,
                          VCSliceStatus::PENDING, 0},
    };
    return record;
}

TEST(VChunkMetadataTest, AcceptsValidLayout) {
    EXPECT_EQ(ValidateVChunkMetadata(MakeValidRecord(), VChunkConfig{}),
              ErrorCode::OK);
}

TEST(VChunkMetadataTest, RoundTripsStableRecord) {
    auto original = MakeValidRecord();
    original.leader_epoch = 7;
    original.metadata_version = 11;
    original.owner_slot = 3;
    original.owner_submaster_id = "submaster-a";
    original.owner_epoch = 8;
    original.route_version = 9;
    original.slices[0].segment_instance_id = "instance-a";
    original.slices[0].allocation_generation = 5;
    original.slices[0].content_checksum = 1234;
    original.slice_groups.push_back(
        SliceGroup{0, "segment-a", 0, {0, 2}});
    auto serialized = SerializeVChunkMetadata(original, VChunkConfig{});
    ASSERT_TRUE(serialized.has_value());

    auto restored =
        DeserializeVChunkMetadata(serialized.value(), VChunkConfig{});
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->schema_version, original.schema_version);
    EXPECT_EQ(restored->vchunk_id, original.vchunk_id);
    EXPECT_EQ(restored->tenant_id, original.tenant_id);
    EXPECT_EQ(restored->key, original.key);
    EXPECT_EQ(restored->total_size, original.total_size);
    EXPECT_EQ(restored->slices.size(), original.slices.size());
    EXPECT_EQ(restored->slices.back().logical_length, 2048U);
    EXPECT_EQ(restored->created_at_ms, 100);
    EXPECT_EQ(restored->leader_epoch, 7U);
    EXPECT_EQ(restored->metadata_version, 11U);
    EXPECT_EQ(restored->owner_slot, 3U);
    EXPECT_EQ(restored->owner_submaster_id, "submaster-a");
    EXPECT_EQ(restored->owner_epoch, 8U);
    EXPECT_EQ(restored->route_version, 9U);
    EXPECT_EQ(restored->slices[0].segment_instance_id, "instance-a");
    EXPECT_EQ(restored->slices[0].content_checksum, 1234U);
    ASSERT_EQ(restored->slice_groups.size(), 1U);
    EXPECT_EQ(restored->slice_groups[0].slice_indices.size(), 2U);
}

TEST(VChunkMetadataTest, UpgradesSchemaVersionTwoRecords) {
    const auto current = MakeValidRecord();
    LegacyRecordV2 legacy;
    legacy.vchunk_id = current.vchunk_id;
    legacy.tenant_id = current.tenant_id;
    legacy.key = current.key;
    legacy.total_size = current.total_size;
    legacy.slice_count = current.slice_count;
    legacy.slice_size_level = current.slice_size_level;
    legacy.slices = current.slices;
    legacy.row_size = current.row_size;
    legacy.status = current.status;
    legacy.created_at_ms = current.created_at_ms;
    legacy.last_updated_at_ms = current.last_updated_at_ms;
    legacy.metadata_version = 4;

    auto restored = DeserializeVChunkMetadata(struct_pack::serialize(legacy),
                                               VChunkConfig{});
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->schema_version, kVChunkMetadataSchemaVersion);
    EXPECT_EQ(restored->metadata_version, 4U);
    EXPECT_EQ(restored->route_version, 0U);
    EXPECT_TRUE(restored->owner_submaster_id.empty());
}

TEST(VChunkMetadataTest, UpgradesSchemaVersionTwoIndexes) {
    LegacyIndexV2 legacy;
    legacy.vchunk_id = "legacy-vchunk";
    legacy.tenant_id = "tenant";
    legacy.key = "key";
    legacy.total_size = 4096;
    legacy.slice_count = 1;
    legacy.row_size = 1;
    legacy.created_at_ms = 10;
    legacy.last_updated_at_ms = 20;

    auto restored = DeserializeVChunkMetadataIndex(
        struct_pack::serialize(legacy), VChunkConfig{});
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->schema_version, kVChunkMetadataSchemaVersion);
    EXPECT_EQ(restored->vchunk_id, "legacy-vchunk");
    EXPECT_EQ(restored->route_version, 0U);
}

TEST(VChunkMetadataTest, RejectsIncompleteOwnershipSnapshot) {
    auto record = MakeValidRecord();
    record.route_version = 2;
    record.owner_epoch = 3;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record.owner_submaster_id = "submaster-a";
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}), ErrorCode::OK);
}

TEST(VChunkMetadataTest, UpgradesSchemaVersionOneRecords) {
    LegacyRecord legacy;
    legacy.vchunk_id = "legacy-vchunk";
    legacy.tenant_id = "tenant";
    legacy.key = "key";
    legacy.total_size = 4096;
    legacy.slice_count = 1;
    legacy.row_size = 1;
    legacy.created_at_ms = 10;
    legacy.last_updated_at_ms = 20;
    legacy.slices.push_back(
        LegacySlice{0, "segment", 0, 4096, 4096,
                    VCSliceStatus::COMPLETED, 0});

    const auto bytes = struct_pack::serialize(legacy);
    auto restored = DeserializeVChunkMetadata(bytes, VChunkConfig{});
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->schema_version, kVChunkMetadataSchemaVersion);
    EXPECT_EQ(restored->vchunk_id, "legacy-vchunk");
    EXPECT_EQ(restored->replica_num, 1U);
    EXPECT_EQ(restored->slices[0].replica_index, 0U);
}

TEST(VChunkMetadataTest, RejectsUnsupportedSchema) {
    auto record = MakeValidRecord();
    record.schema_version = kVChunkMetadataSchemaVersion + 1;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_VERSION);
}

TEST(VChunkMetadataTest, RejectsGapsAndDuplicateSegmentsWithinRow) {
    auto record = MakeValidRecord();
    record.slices[1].slice_index = 3;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices[1].target_segment_name = "segment-a";
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RejectsIncorrectCoverageAndAllocation) {
    auto record = MakeValidRecord();
    record.slices.back().logical_length = 1024;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices.back().allocated_length = 1024;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RejectsOffsetOverflowAndRetryOverflow) {
    auto record = MakeValidRecord();
    record.slices[0].target_offset =
        std::numeric_limits<uint64_t>::max() - 1024;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices[0].retry_count = VChunkConfig{}.max_slice_retry + 1;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RejectsInvalidReplicaAndSliceGroup) {
    auto record = MakeValidRecord();
    record.replica_num = 0;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices[0].replica_index = 1;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slice_groups.push_back(SliceGroup{0, "segment-a", 0, {3}});
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, BuildsIndexAndPartitionsBySegment) {
    auto record = MakeValidRecord();
    record.metadata_version = 9;
    const auto index = BuildVChunkMetadataIndex(record);
    EXPECT_EQ(index.vchunk_id, record.vchunk_id);
    EXPECT_EQ(index.metadata_version, 9U);
    EXPECT_EQ(index.slice_count, 3U);

    const auto partitions = PartitionVChunkSlices(record);
    ASSERT_EQ(partitions.size(), 2U);
    EXPECT_EQ(partitions[0].segment_name, "segment-a");
    ASSERT_EQ(partitions[0].slices.size(), 2U);
    EXPECT_EQ(partitions[0].slices[0].slice_index, 0U);
    EXPECT_EQ(partitions[0].slices[1].slice_index, 2U);
    EXPECT_EQ(partitions[1].segment_name, "segment-b");
    ASSERT_EQ(partitions[1].slices.size(), 1U);
}

TEST(VChunkMetadataTest, RoundTripsIndexAndPartitionLayout) {
    auto record = MakeValidRecord();
    record.metadata_version = 4;
    auto encoded_index =
        SerializeVChunkMetadataIndex(BuildVChunkMetadataIndex(record),
                                     VChunkConfig{});
    ASSERT_TRUE(encoded_index.has_value());
    auto index =
        DeserializeVChunkMetadataIndex(*encoded_index, VChunkConfig{});
    ASSERT_TRUE(index.has_value());

    std::vector<VCSlicePartition> restored_partitions;
    for (const auto& partition : PartitionVChunkSlices(record)) {
        auto encoded =
            SerializeVChunkSlicePartition(partition, VChunkConfig{});
        ASSERT_TRUE(encoded.has_value());
        auto restored =
            DeserializeVChunkSlicePartition(*encoded, VChunkConfig{});
        ASSERT_TRUE(restored.has_value());
        restored_partitions.push_back(std::move(*restored));
    }
    auto assembled = AssembleVChunkMetadata(
        std::move(*index), std::move(restored_partitions), VChunkConfig{});
    ASSERT_TRUE(assembled.has_value());
    EXPECT_EQ(assembled->metadata_version, 4U);
    ASSERT_EQ(assembled->slices.size(), record.slices.size());
    for (size_t i = 0; i < record.slices.size(); ++i) {
        EXPECT_EQ(assembled->slices[i].slice_index,
                  record.slices[i].slice_index);
        EXPECT_EQ(assembled->slices[i].target_segment_name,
                  record.slices[i].target_segment_name);
    }
}

TEST(VChunkMetadataTest, RejectsMismatchedSlicePartition) {
    auto record = MakeValidRecord();
    auto partitions = PartitionVChunkSlices(record);
    partitions[0].slices[0].target_segment_name = "other";
    auto encoded =
        SerializeVChunkSlicePartition(partitions[0], VChunkConfig{});
    ASSERT_FALSE(encoded.has_value());
    EXPECT_EQ(encoded.error(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, EnforcesMetadataSizeLimit) {
    auto config = VChunkConfig{};
    config.max_metadata_bytes = 8;
    auto serialized = SerializeVChunkMetadata(MakeValidRecord(), config);
    ASSERT_FALSE(serialized.has_value());
    EXPECT_EQ(serialized.error(), ErrorCode::BUFFER_OVERFLOW);
}

TEST(VChunkMetadataTest, RejectsCorruptedBytes) {
    auto serialized =
        SerializeVChunkMetadata(MakeValidRecord(), VChunkConfig{});
    ASSERT_TRUE(serialized.has_value());
    serialized->pop_back();

    auto restored =
        DeserializeVChunkMetadata(serialized.value(), VChunkConfig{});
    EXPECT_FALSE(restored.has_value());
    EXPECT_EQ(restored.error(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RuntimeStateTransitionsAreValidated) {
    VChunkMetadata metadata(MakeValidRecord());
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::ACTIVE, 200), ErrorCode::OK);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::CREATING, 300),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::RELEASING, 199),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::RELEASING, 300),
              ErrorCode::OK);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::RELEASED, 400),
              ErrorCode::OK);

    const auto snapshot = metadata.Snapshot();
    EXPECT_EQ(snapshot.status, VChunkStatus::RELEASED);
    EXPECT_EQ(snapshot.last_updated_at_ms, 400);
}

TEST(VChunkMetadataTest, RecoveryStateTransitionsAreValidated) {
    EXPECT_EQ(ValidateVChunkTransition(VChunkStatus::CREATING,
                                       VChunkStatus::RECOVERING),
              ErrorCode::OK);
    EXPECT_EQ(ValidateVChunkTransition(VChunkStatus::ACTIVE,
                                       VChunkStatus::RECOVERING),
              ErrorCode::OK);
    EXPECT_EQ(ValidateVChunkTransition(VChunkStatus::RECOVERING,
                                       VChunkStatus::ACTIVE),
              ErrorCode::OK);
    EXPECT_EQ(ValidateVChunkTransition(VChunkStatus::RECOVERING,
                                       VChunkStatus::ABANDONED),
              ErrorCode::OK);
    EXPECT_EQ(ValidateVChunkTransition(VChunkStatus::ABANDONED,
                                       VChunkStatus::ACTIVE),
              ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake
