#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"
#include "vsegment/vsegment_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

namespace mooncake::vsegment {
namespace {

VSegmentProfile Profile(uint32_t members = 2) {
    return {.name = "default",
            .member_count = members,
            .stripe_size = 64,
            .member_extent_size = 256,
            .io_alignment = 8,
            .required_medium = "DRAM"};
}

PartitionVSegmentConfig Config() {
    return {.partition_id = "partition-1",
            .profile_name = "default",
            .initial_vsegment_count = 1,
            .config_generation = 1,
            .quotas = {{"segment-a", 0, 512}, {"segment-b", 1024, 512}}};
}

TEST(VSegmentConfigTest, RejectsInvalidLayoutAndOverlappingQuota) {
    auto profile = Profile();
    profile.member_extent_size = 100;
    EXPECT_EQ(ValidateProfile(profile), ErrorCode::INVALID_PARAMS);

    auto config = Config();
    config.quotas.push_back({"segment-a", 128, 64});
    EXPECT_EQ(ValidatePartitionConfig(config), ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentConfigTest, ValidatesPublishedConfigAgainstSegmentGeometry) {
    auto profile = Profile();
    auto config = Config();
    std::vector<PSegmentGeometry> segments = {
        {"segment-a", 2048, 8, "DRAM"},
        {"segment-b", 2048, 8, "DRAM"}};
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config}, segments, {}),
              ErrorCode::OK);

    auto overlapping = config;
    overlapping.partition_id = "partition-2";
    overlapping.config_generation = 2;
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config, overlapping},
                                      segments, {}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ValidatePublishedConfig({profile}, {config}, segments,
                                      {{"partition-1", 1}}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentViewTest, ChecksumCoversOrderedMembers) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(ValidateView(allocation.view, Profile()), ErrorCode::OK);

    std::swap(allocation.view.members[0], allocation.view.members[1]);
    EXPECT_EQ(ValidateView(allocation.view, Profile()),
              ErrorCode::CHECKSUM_MISMATCH);
}

TEST(VSegmentViewTest, LifecycleDoesNotChangeImmutableViewChecksum) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentStateSnapshot state{"default", "partition-1/default/0",
                                Lifecycle::ACTIVE,
                                allocation.view, {}};
    const auto checksum = state.view.checksum;
    state.lifecycle = Lifecycle::DRAINING;
    EXPECT_EQ(state.view.checksum, checksum);
    EXPECT_EQ(ValidateView(state.view, Profile()), ErrorCode::OK);
}

TEST(PartitionQuotaAllocatorTest, AllocatesAndReleasesStaticQuota) {
    PartitionQuotaAllocator allocator(Config());
    auto first = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(first);
    ASSERT_EQ(first.view.members.size(), 2);
    EXPECT_EQ(first.view.members[0],
              (PSegmentExtent{"segment-a", 0, 256}));
    EXPECT_EQ(first.view.members[1],
              (PSegmentExtent{"segment-b", 1024, 256}));
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 256);

    EXPECT_EQ(allocator.Release(first.view), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 512);
    EXPECT_EQ(allocator.FreeBytes("segment-b"), 512);
}

TEST(PartitionQuotaAllocatorTest, DoesNotDegradeConfiguredMemberCount) {
    auto config = Config();
    config.quotas.pop_back();
    PartitionQuotaAllocator allocator(config);
    auto allocation = allocator.Allocate("vs-1", Profile());
    EXPECT_EQ(allocation.error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(allocation.detail.find("requires 2 psegments"),
              std::string::npos);
    EXPECT_EQ(allocator.FreeBytes("segment-a"), 512);
}

TEST(PartitionQuotaAllocatorTest, ConcurrentIdentityIsIdempotentlyRejected) {
    PartitionQuotaAllocator allocator(Config());
    ASSERT_TRUE(allocator.Allocate("vs-1", Profile()));
    auto duplicate = allocator.Allocate("vs-1", Profile());
    EXPECT_EQ(duplicate.error, ErrorCode::SEGMENT_ALREADY_EXISTS);
}

TEST(LogicalRangeAllocatorTest, ReservationIsIdempotentAndAbortReturnsSpace) {
    LogicalRangeAllocator allocator(1024);
    auto first = allocator.Reserve("put-1", 128);
    ASSERT_TRUE(first);
    auto retry = allocator.Reserve("put-1", 128);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.range.offset, first.range.offset);
    EXPECT_EQ(allocator.FreeBytes(), 896);
    EXPECT_EQ(allocator.ReservationCount(), 1);
    EXPECT_EQ(allocator.Abort("put-1"), ErrorCode::OK);
    EXPECT_EQ(allocator.Abort("put-1"), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes(), 1024);
}

TEST(LogicalRangeAllocatorTest, ReleaseRequiresExactObjectAllocation) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    LogicalRange committed;
    ASSERT_EQ(allocator.Commit("put-1", "object-1", &committed),
              ErrorCode::OK);
    EXPECT_EQ(allocator.Release("object-2", committed),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(allocator.Release("object-1",
                                {committed.offset, committed.length / 2}),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(allocator.FreeBytes(), 896);
}

TEST(LogicalRangeAllocatorTest, CommitKeepsRangeAllocatedUntilRelease) {
    LogicalRangeAllocator allocator(1024);
    ASSERT_TRUE(allocator.Reserve("put-1", 128));
    LogicalRange committed;
    EXPECT_EQ(allocator.Commit("put-1", "object-1", &committed),
              ErrorCode::OK);
    LogicalRange retried;
    EXPECT_EQ(allocator.Commit("put-1", "object-1", &retried),
              ErrorCode::OK);
    EXPECT_EQ(retried.offset, committed.offset);
    EXPECT_EQ(allocator.FreeBytes(), 896);
    EXPECT_EQ(allocator.Release("object-1", committed), ErrorCode::OK);
    EXPECT_EQ(allocator.FreeBytes(), 1024);
    EXPECT_EQ(allocator.Release("object-1", committed),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST(VSegmentResolverTest, SplitsAtStripeAndClientSliceBoundaries) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);

    auto resolved = ResolveTransfer(allocation.view, 48, 96,
                                    {{1000, 40}, {2000, 56}});
    ASSERT_TRUE(resolved);
    ASSERT_EQ(resolved.requests.size(), 4);
    EXPECT_EQ(resolved.requests[0].length, 16);
    EXPECT_EQ(resolved.requests[0].segment_id, "segment-a");
    EXPECT_EQ(resolved.requests[0].physical_offset, 48);
    EXPECT_EQ(resolved.requests[1].length, 24);
    EXPECT_EQ(resolved.requests[1].segment_id, "segment-b");
    EXPECT_EQ(resolved.requests[1].client_address, 1016);
    EXPECT_EQ(resolved.requests[2].length, 40);
    EXPECT_EQ(resolved.requests[2].client_address, 2000);
    EXPECT_EQ(resolved.requests[3].length, 16);
    EXPECT_EQ(resolved.requests[3].segment_id, "segment-a");
    EXPECT_EQ(resolved.requests[3].physical_offset, 64);
    EXPECT_EQ(resolved.requests[3].client_buffer_offset, 80);
}

TEST(VSegmentResolverTest, RejectsViewWithInvalidChecksum) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    allocation.view.members[0].base_offset += 8;
    auto resolved = ResolveTransfer(allocation.view, 0, 8, {{1000, 8}});
    EXPECT_EQ(resolved.error, ErrorCode::CHECKSUM_MISMATCH);
}

TEST(PartitionQuotaAllocatorTest, PreservesConfiguredMemberOrder) {
    auto config = Config();
    std::swap(config.quotas[0], config.quotas[1]);
    PartitionQuotaAllocator allocator(config);
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(allocation.view.members[0].segment_id, "segment-b");
    EXPECT_EQ(allocation.view.members[1].segment_id, "segment-a");
}

TEST(CreationCoordinatorTest, ConcurrentCallersShareOneCreation) {
    CreationCoordinator coordinator;
    std::atomic<int> calls{0};
    VSegmentAllocationResult first;
    VSegmentAllocationResult second;
    auto factory = [&] {
        ++calls;
        std::this_thread::yield();
        VSegmentAllocationResult result;
        result.view.vsegment_id = "vs-1";
        return result;
    };
    std::thread one([&] { first = coordinator.GetOrCreate("p/default/0", factory); });
    std::thread two([&] { second = coordinator.GetOrCreate("p/default/0", factory); });
    one.join();
    two.join();
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(first.view.vsegment_id, "vs-1");
    EXPECT_EQ(second.view.vsegment_id, "vs-1");
}

TEST(VSegmentManagerTest, SnapshotRestorePreservesReservationsAndFreeSpace) {
    VSegmentManager manager(Config(), {Profile()});
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto reservation = manager.ReservePut(allocation.view.vsegment_id,
                                          "put-1", 128);
    ASSERT_TRUE(reservation);

    const auto snapshot = manager.Snapshot();
    ASSERT_EQ(snapshot.vsegments.size(), 1);
    VSegmentManager recovered(Config(), {Profile()});
    std::string detail;
    ASSERT_EQ(recovered.Restore(snapshot, &detail), ErrorCode::OK) << detail;

    VSegmentView restored_view;
    EXPECT_TRUE(recovered.FindView(allocation.view.vsegment_id,
                                   &restored_view));
    EXPECT_EQ(restored_view.checksum, allocation.view.checksum);
    LogicalRange committed;
    EXPECT_EQ(recovered.CommitPut(allocation.view.vsegment_id, "put-1",
                                  "object-1", &committed),
              ErrorCode::OK);
    EXPECT_EQ(committed.offset, reservation.range.offset);
    EXPECT_EQ(committed.length, reservation.range.length);
}

TEST(VSegmentManagerTest, RejectsSnapshotFromAnotherConfigGeneration) {
    VSegmentManager manager(Config(), {Profile()});
    ASSERT_TRUE(manager.Create("default"));
    auto snapshot = manager.Snapshot();
    snapshot.config_generation = 2;

    VSegmentManager recovered(Config(), {Profile()});
    EXPECT_EQ(recovered.Restore(snapshot), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentManagerTest, EnforcesLifecycleTransitions) {
    VSegmentManager manager(Config(), {Profile()});
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    EXPECT_FALSE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 8));
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, RestoresCommittedIdentityForExactRelease) {
    VSegmentManager manager(Config(), {Profile()});
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    LogicalRange range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "put-1",
                                "object-1", &range),
              ErrorCode::OK);
    auto snapshot = manager.Snapshot();

    VSegmentManager recovered(Config(), {Profile()});
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    EXPECT_EQ(recovered.ReleaseObject(allocation.view.vsegment_id,
                                      "object-1", range),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, SnapshotIsJsonSerializable) {
    VSegmentManager manager(Config(), {Profile()});
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));

    std::string json;
    struct_json::to_json(manager.Snapshot(), json);
    PartitionVSegmentSnapshot decoded;
    struct_json::from_json(decoded, json);
    ASSERT_EQ(decoded.vsegments.size(), 1);
    EXPECT_EQ(decoded.partition_id, "partition-1");
    EXPECT_EQ(decoded.vsegments[0].logical_allocation.reservations.size(), 1);
}

}  // namespace
}  // namespace mooncake::vsegment
