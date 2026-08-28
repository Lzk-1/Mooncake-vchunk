#include "vchunk_recovery.h"

#include <gtest/gtest.h>

#include "allocation_strategy.h"
#include "vchunk_master_manager.h"

namespace mooncake {
namespace {

VChunkMetadataRecord MakeRecoveryRecord(uintptr_t address,
                                        std::string instance_id) {
    VChunkMetadataRecord record;
    record.vchunk_id = "recovered-id";
    record.tenant_id = "tenant";
    record.key = "key";
    record.total_size = 4096;
    record.slice_count = 1;
    record.row_size = 1;
    record.status = VChunkStatus::ACTIVE;
    record.created_at_ms = 1;
    record.last_updated_at_ms = 2;
    record.leader_epoch = 4;
    record.metadata_version = 3;
    VCSliceDescriptor slice;
    slice.target_segment_name = "segment";
    slice.segment_instance_id = std::move(instance_id);
    slice.target_offset = address;
    slice.logical_length = 4096;
    slice.allocated_length = 4096;
    slice.allocation_generation = 1;
    slice.content_checksum = 123;
    slice.status = VCSliceStatus::COMPLETED;
    record.slices.push_back(std::move(slice));
    return record;
}

TEST(VChunkRecoveryTest, ClaimsIntoIsolatedViewAndAdvancesEpoch) {
    constexpr uintptr_t kBase = 0x1B0000000ULL;
    AllocatorManager allocators;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment", kBase, 64 * 1024, "endpoint", ReplicaType::MEMORY,
        "instance-1");
    allocators.addAllocator("segment", allocator);
    VChunkRecoveryManager recovery(VChunkConfig{});

    auto view = recovery.BuildIsolatedView(
        {MakeRecoveryRecord(kBase, "instance-1")}, allocators, 5,
        [](const auto&, const auto&) { return ErrorCode::OK; });
    ASSERT_TRUE(view.has_value());
    ASSERT_EQ(view->entries.size(), 1U);
    ASSERT_EQ(view->entries[0].claims.size(), 1U);
    EXPECT_EQ(view->entries[0].record.leader_epoch, 5U);
    EXPECT_EQ(view->entries[0].record.metadata_version, 4U);
    EXPECT_EQ(recovery.Phase(),
              VChunkRecoveryPhase::INCOMPLETE_RECOVERING);
    EXPECT_EQ(allocator->size(), 4096U);
}

TEST(VChunkRecoveryTest, RejectsRestartedSegmentWithoutAllocating) {
    constexpr uintptr_t kBase = 0x1C0000000ULL;
    AllocatorManager allocators;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment", kBase, 64 * 1024, "endpoint", ReplicaType::MEMORY,
        "new-instance");
    allocators.addAllocator("segment", allocator);
    VChunkRecoveryManager recovery(VChunkConfig{});

    auto view = recovery.BuildIsolatedView(
        {MakeRecoveryRecord(kBase, "old-instance")}, allocators, 5,
        [](const auto&, const auto&) { return ErrorCode::OK; });
    ASSERT_FALSE(view.has_value());
    EXPECT_EQ(view.error(), ErrorCode::REPLICA_IS_GONE);
    EXPECT_EQ(recovery.Phase(), VChunkRecoveryPhase::FAILED);
    EXPECT_EQ(allocator->size(), 0U);
}

TEST(VChunkRecoveryTest, PublishesRecoveredViewAtomically) {
    constexpr uintptr_t kBase = 0x1D0000000ULL;
    AllocatorManager allocators;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment", kBase, 64 * 1024, "endpoint", ReplicaType::MEMORY,
        "instance-1");
    allocators.addAllocator("segment", allocator);
    VChunkRecoveryManager recovery(VChunkConfig{});
    auto view = recovery.BuildIsolatedView(
        {MakeRecoveryRecord(kBase, "instance-1")}, allocators, 5,
        [](const auto&, const auto&) { return ErrorCode::OK; });
    ASSERT_TRUE(view.has_value());

    VChunkConfig config;
    config.enabled = true;
    VChunkMasterManager manager(config);
    manager.DeactivateLeader();
    ASSERT_EQ(manager.PublishRecoveryView(std::move(*view)), ErrorCode::OK);
    auto record = manager.Get(TenantId("tenant"), "key");
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->leader_epoch, 5U);
}

}  // namespace
}  // namespace mooncake
