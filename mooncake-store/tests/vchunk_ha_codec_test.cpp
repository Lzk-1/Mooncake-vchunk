#include "vchunk_ha_codec.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

VChunkMetadataRecord MakeRecord(uint64_t version = 1) {
    VChunkMetadataRecord record;
    record.vchunk_id = "id";
    record.tenant_id = "tenant";
    record.key = "key";
    record.total_size = 4096;
    record.slice_count = 1;
    record.row_size = 1;
    record.created_at_ms = 1;
    record.last_updated_at_ms = 2;
    record.metadata_version = version;
    record.leader_epoch = 7;
    record.slices.push_back({0, "segment", 0, 4096, 4096,
                             VCSliceStatus::PENDING, 0});
    return record;
}

TEST(VChunkHACodecTest, RoundTripsEventAndSnapshot) {
    VChunkHAEvent event;
    event.sequence_id = 3;
    event.leader_epoch = 7;
    event.record = MakeRecord();
    auto bytes = SerializeVChunkHAEvent(event, VChunkConfig{});
    ASSERT_TRUE(bytes.has_value());
    auto restored = DeserializeVChunkHAEvent(*bytes, VChunkConfig{});
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->sequence_id, 3U);
    EXPECT_EQ(restored->record.vchunk_id, "id");

    VChunkSnapshot snapshot;
    snapshot.last_sequence_id = 3;
    snapshot.leader_epoch = 7;
    snapshot.records.push_back(MakeRecord());
    auto snapshot_bytes =
        SerializeVChunkSnapshot(snapshot, VChunkConfig{});
    ASSERT_TRUE(snapshot_bytes.has_value());
    auto restored_snapshot =
        DeserializeVChunkSnapshot(*snapshot_bytes, VChunkConfig{});
    ASSERT_TRUE(restored_snapshot.has_value());
    EXPECT_EQ(restored_snapshot->records.size(), 1U);
}

TEST(VChunkHACodecTest, ReplayIsIdempotentAndRejectsGaps) {
    VChunkRecoveryEntries entries;
    uint64_t sequence = 0;
    VChunkHAEvent create;
    create.sequence_id = 1;
    create.leader_epoch = 7;
    create.record = MakeRecord();
    EXPECT_EQ(ApplyVChunkHAEvent(create, entries, sequence, VChunkConfig{}),
              ErrorCode::OK);
    EXPECT_EQ(entries.size(), 1U);
    EXPECT_EQ(ApplyVChunkHAEvent(create, entries, sequence, VChunkConfig{}),
              ErrorCode::OK);
    EXPECT_EQ(entries.size(), 1U);

    auto gap = create;
    gap.sequence_id = 3;
    gap.record.metadata_version = 2;
    EXPECT_EQ(ApplyVChunkHAEvent(gap, entries, sequence, VChunkConfig{}),
              ErrorCode::INVALID_VERSION);

    auto activate = gap;
    activate.sequence_id = 2;
    activate.type = VChunkHAEventType::ACTIVATE;
    activate.record.status = VChunkStatus::ACTIVE;
    for (auto& slice : activate.record.slices) {
        slice.status = VCSliceStatus::COMPLETED;
    }
    EXPECT_EQ(
        ApplyVChunkHAEvent(activate, entries, sequence, VChunkConfig{}),
        ErrorCode::OK);
    EXPECT_EQ(sequence, 2U);
}

TEST(VChunkHACodecTest, ReleasedEventRemovesEntry) {
    VChunkRecoveryEntries entries;
    uint64_t sequence = 0;
    VChunkHAEvent create;
    create.sequence_id = 1;
    create.leader_epoch = 7;
    create.record = MakeRecord();
    ASSERT_EQ(ApplyVChunkHAEvent(create, entries, sequence, VChunkConfig{}),
              ErrorCode::OK);
    auto released = create;
    released.sequence_id = 2;
    released.type = VChunkHAEventType::RELEASED;
    released.record.metadata_version = 2;
    released.record.status = VChunkStatus::RELEASED;
    EXPECT_EQ(ApplyVChunkHAEvent(released, entries, sequence, VChunkConfig{}),
              ErrorCode::OK);
    EXPECT_TRUE(entries.empty());
}

TEST(VChunkHACodecTest, AllowsOuterOpLogToAssignEventSequence) {
    VChunkHAEvent event;
    event.sequence_id = 0;
    event.leader_epoch = 7;
    event.record = MakeRecord();
    auto encoded = SerializeVChunkHAEvent(event, VChunkConfig{});
    ASSERT_TRUE(encoded.has_value());
    auto decoded = DeserializeVChunkHAEvent(*encoded, VChunkConfig{});
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sequence_id, 0U);

    VChunkRecoveryEntries entries;
    uint64_t applied = 0;
    EXPECT_EQ(ApplyVChunkHAEvent(*decoded, entries, applied, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake
