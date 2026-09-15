#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"
#include "vsegment/vsegment_transfer.h"
#include "cvm/cvm_keys.h"
#include "vsegment/vsegment_ha.h"
#include "vsegment/vsegment_service.h"
#include "vsegment/vsegment_manager.h"
#include "vsegment/partition_quota_planner.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <thread>

namespace mooncake::vsegment {
namespace {

TEST(VSegmentKeysTest, PartitionQuotaSnapshotIsClusterScoped) {
    EXPECT_EQ(cvm::VSegmentPartitionQuotaSnapshotKey("cluster-a"),
              "/cvm/cluster-a/snapshot/vsegment_partition_quota");
    EXPECT_NE(cvm::VSegmentPartitionQuotaSnapshotKey("cluster-a"),
              cvm::VSegmentPartitionQuotaSnapshotKey("cluster-b"));
}

class TestStateCommitter : public VSegmentStateCommitter {
   public:
    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* detail) override {
        mutations.push_back(mutation);
        last_state = state;
        if (delay_create_begin && mutation == "vsegment_create_begin")
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (fail_next) {
            fail_next = false;
            if (detail) *detail = "injected persistence failure";
            return ErrorCode::PERSISTENT_FAIL;
        }
        return ErrorCode::OK;
    }

    bool fail_next{false};
    bool delay_create_begin{false};
    std::vector<std::string> mutations;
    PartitionVSegmentSnapshot last_state;
};

class TestViewProvider : public VSegmentViewProvider {
   public:
    ErrorCode LoadView(const std::string& partition_id,
                       const std::string& vsegment_id, VSegmentView* output,
                       std::string*) override {
        ++loads;
        if (view.partition_id != partition_id ||
            view.vsegment_id != vsegment_id)
            return ErrorCode::SEGMENT_NOT_FOUND;
        *output = view;
        return ErrorCode::OK;
    }
    int loads{0};
    VSegmentView view;
};

class TestEndpointResolver : public SegmentEndpointResolver {
   public:
    ErrorCode ResolveEndpoint(const std::string& segment_id,
                              std::string* endpoint) override {
        *endpoint = "endpoint://" + segment_id;
        return ErrorCode::OK;
    }
};

std::shared_ptr<TestStateCommitter> Committer() {
    return std::make_shared<TestStateCommitter>();
}

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

TEST(PartitionQuotaPlannerTest, SplitsEveryMediumAcrossAllPartitions) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 7;
    request.policy_digest = "policy-v7";
    request.default_profile = "dram";
    request.partition_ids = {"partition-b", "partition-a"};
    request.profile_specs = {
        {.name = "dram",
         .member_count = 2,
         .stripe_size = 64,
         .member_extent_size = 128,
         .io_alignment = 8,
         .required_medium = "DRAM"},
        {.name = "nvme",
         .member_count = 2,
         .stripe_size = 64,
         .member_extent_size = 128,
         .io_alignment = 8,
         .required_medium = "NVMe"}};
    request.segments = {{"dram-a", 1024, 8, "DRAM"},
                        {"dram-b", 1024, 8, "DRAM"},
                        {"nvme-a", 2048, 8, "NVMe"},
                        {"nvme-b", 2048, 8, "NVMe"}};

    auto result = PartitionQuotaPlanner().Plan(request);
    ASSERT_TRUE(result) << result.detail;
    ASSERT_EQ(result.snapshot.quotas.size(), 4);
    EXPECT_EQ(result.snapshot.quotas[0].partition_id, "partition-a");
    EXPECT_EQ(result.snapshot.quotas[0].profile_name, "dram");
    EXPECT_EQ(result.snapshot.quotas[0].extents[0].base_offset, 0);
    EXPECT_EQ(result.snapshot.quotas[1].extents[0].base_offset, 512);
    EXPECT_EQ(result.snapshot.quotas[2].profile_name, "nvme");
    EXPECT_EQ(ValidateQuotaSnapshot(result.snapshot, request.segments),
              ErrorCode::OK);

    PartitionVSegmentConfig config;
    EXPECT_EQ(BuildPartitionConfig(result.snapshot, "partition-a", "nvme",
                                   &config),
              ErrorCode::OK);
    EXPECT_EQ(config.quotas.size(), 2);
    EXPECT_EQ(config.config_generation, 7);
}

TEST(PartitionQuotaPlannerTest, RejectsInsufficientMemberSegments) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"only-one", 4096, 8, "DRAM"}};
    auto result = PartitionQuotaPlanner().Plan(request);
    EXPECT_EQ(result.error, ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    EXPECT_NE(result.detail.find("requires 2 psegments"), std::string::npos);
}

TEST(PartitionQuotaPlannerTest, ExcludesUnhealthySegments) {
    PartitionQuotaPlanRequest request;
    request.config_generation = 1;
    request.policy_digest = "policy";
    request.default_profile = "default";
    request.partition_ids = {"partition-a"};
    request.profile_specs = {Profile(2)};
    request.segments = {{"healthy", 4096, 8, "DRAM", true, true, "host-a"},
                        {"unhealthy", 4096, 8, "DRAM", false, true,
                         "host-b"}};
    EXPECT_EQ(PartitionQuotaPlanner().Plan(request).error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
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
    VSegmentStateSnapshot state{"default", Lifecycle::ACTIVE,
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

TEST(VSegmentTransferPlannerTest, LoadsImmutableViewAndResolvesEndpoints) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    TestViewProvider provider;
    provider.view = allocation.view;
    TestEndpointResolver endpoints;
    VSegmentViewCache cache;
    VSegmentTransferPlanner planner(&provider, &endpoints, &cache);
    VSegmentDescriptor replica{"partition-1", "vs-1", 48, 96};

    auto first = planner.Plan(replica, {{1000, 96}});
    ASSERT_TRUE(first) << first.detail;
    ASSERT_EQ(first.requests.size(), 3);
    EXPECT_EQ(first.requests[0].endpoint, "endpoint://segment-a");
    EXPECT_EQ(first.requests[1].endpoint, "endpoint://segment-b");
    EXPECT_EQ(provider.loads, 1);

    auto cached = planner.Plan(replica, {{2000, 96}});
    ASSERT_TRUE(cached);
    EXPECT_EQ(provider.loads, 1);
}

TEST(VSegmentTransferPlannerTest, RejectsConflictingCachedView) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-1", Profile());
    ASSERT_TRUE(allocation);
    VSegmentViewCache cache;
    ASSERT_EQ(cache.Insert(allocation.view), ErrorCode::OK);
    auto conflicting = allocation.view;
    conflicting.members[0].base_offset += 8;
    conflicting.checksum = ComputeViewChecksum(conflicting);
    EXPECT_EQ(cache.Insert(conflicting), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentViewCacheTest, ScopesIdentityByPartition) {
    auto first = View();
    auto second = first;
    second.partition_id = "partition-2";
    second.stripe_size *= 2;
    second.checksum = ComputeViewChecksum(second);

    VSegmentViewCache cache;
    ASSERT_EQ(cache.Insert(first), ErrorCode::OK);
    ASSERT_EQ(cache.Insert(second), ErrorCode::OK);
    VSegmentView loaded;
    ASSERT_TRUE(cache.Find(first.partition_id, first.vsegment_id, &loaded));
    EXPECT_EQ(loaded.stripe_size, first.stripe_size);
    ASSERT_TRUE(cache.Find(second.partition_id, second.vsegment_id, &loaded));
    EXPECT_EQ(loaded.stripe_size, second.stripe_size);
}

TEST(VSegmentViewTest, PersistsCreatingProfileIdentity) {
    PartitionQuotaAllocator allocator(Config());
    auto allocation = allocator.Allocate("vs-profile", Profile());
    ASSERT_TRUE(allocation);
    EXPECT_EQ(allocation.view.profile_name, "default");
    EXPECT_EQ(ValidateView(allocation.view, Profile()), ErrorCode::OK);

    auto wrong_profile = Profile();
    wrong_profile.name = "other";
    EXPECT_EQ(ValidateView(allocation.view, wrong_profile),
              ErrorCode::INVALID_PARAMS);
}

TEST(VSegmentReplicaTest, RuntimeMetadataPreservesLogicalDescriptor) {
    VSegmentDescriptor expected{"partition-1", "vs-1", 128, 64};
    Replica replica(expected, ReplicaStatus::COMPLETE);

    EXPECT_EQ(replica.type(), ReplicaType::VSEGMENT);
    EXPECT_TRUE(replica.is_vsegment_replica());
    const auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_vsegment_replica());
    const auto& actual = descriptor.get_vsegment_descriptor();
    EXPECT_EQ(actual.partition_id, expected.partition_id);
    EXPECT_EQ(actual.vsegment_id, expected.vsegment_id);
    EXPECT_EQ(actual.logical_offset, expected.logical_offset);
    EXPECT_EQ(actual.length, expected.length);
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
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto reservation = manager.ReservePut(allocation.view.vsegment_id,
                                          "put-1", 128);
    ASSERT_TRUE(reservation);

    const auto snapshot = manager.Snapshot();
    ASSERT_EQ(snapshot.vsegments.size(), 1);
    VSegmentManager recovered(Config(), {Profile()}, Committer());
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
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_TRUE(manager.Create("default"));
    auto snapshot = manager.Snapshot();
    snapshot.config_generation = 2;

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    EXPECT_EQ(recovered.Restore(snapshot), ErrorCode::INVALID_VERSION);
}

TEST(VSegmentManagerTest, EnforcesLifecycleTransitions) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
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

TEST(VSegmentManagerTest, RefusesToRetireReferencedVSegment) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OBJECT_REPLICA_BUSY);
}

TEST(VSegmentManagerTest, RetireReturnsExtentsToPartitionQuota) {
    auto config = Config();
    config.quotas[0].length = 256;
    config.quotas[1].length = 256;
    VSegmentManager manager(config, {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    EXPECT_EQ(manager.Create("default").error,
              ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::DRAINING),
              ErrorCode::OK);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
    EXPECT_TRUE(manager.Create("default"));
}

TEST(VSegmentManagerTest, ConsumesMultipleProfilesFromPublishedSnapshot) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 3;
    snapshot.policy_digest = "policy-v3";
    snapshot.default_profile = "dram";
    auto dram = Profile();
    dram.name = "dram";
    auto nvme = Profile();
    nvme.name = "nvme";
    nvme.required_medium = "NVMe";
    snapshot.profile_specs = {dram, nvme};
    snapshot.quotas = {
        {"partition-1", "dram", "DRAM",
         {{"dram-a", 0, 256}, {"dram-b", 0, 256}}},
        {"partition-1", "nvme", "NVMe",
         {{"nvme-a", 0, 256}, {"nvme-b", 0, 256}}}};

    VSegmentManager manager(snapshot, "partition-1", Committer());
    auto dram_view = manager.Create("dram");
    auto nvme_view = manager.Create("nvme");
    ASSERT_TRUE(dram_view);
    ASSERT_TRUE(nvme_view);
    EXPECT_EQ(dram_view.view.members[0].segment_id, "dram-a");
    EXPECT_EQ(nvme_view.view.members[0].segment_id, "nvme-a");
    EXPECT_EQ(manager.Snapshot().vsegments.size(), 2);
}

TEST(VSegmentManagerTest, RestoresCommittedIdentityForExactRelease) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    LogicalRange range;
    ASSERT_EQ(manager.CommitPut(allocation.view.vsegment_id, "put-1",
                                "object-1", &range),
              ErrorCode::OK);
    auto snapshot = manager.Snapshot();

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    EXPECT_EQ(recovered.ReleaseObject(allocation.view.vsegment_id,
                                      "object-1", range),
              ErrorCode::OK);
}

TEST(VSegmentManagerTest, SnapshotIsJsonSerializable) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
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

TEST(VSegmentManagerTest, PersistsCreationBeforePublishingActive) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_EQ(committer->mutations.size(), 2);
    EXPECT_EQ(committer->mutations[0], "vsegment_create_begin");
    EXPECT_EQ(committer->mutations[1], "vsegment_create_commit");
    ASSERT_EQ(committer->last_state.vsegments.size(), 1);
    EXPECT_EQ(committer->last_state.vsegments[0].lifecycle,
              Lifecycle::ACTIVE);
}

TEST(VSegmentManagerTest, RollsBackReservationWhenPersistenceFails) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    committer->fail_next = true;
    auto failed = manager.ReservePut(allocation.view.vsegment_id, "put-1", 64);
    EXPECT_EQ(failed.error, ErrorCode::PERSISTENT_FAIL);

    auto retry = manager.ReservePut(allocation.view.vsegment_id, "put-1", 64);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.range.offset, 0);
}

TEST(VSegmentManagerTest, RecoveryRollsBackUncommittedCreation) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    auto snapshot = manager.Snapshot();
    snapshot.vsegments[0].lifecycle = Lifecycle::PREPARING;

    VSegmentManager recovered(Config(), {Profile()}, Committer());
    ASSERT_EQ(recovered.Restore(snapshot), ErrorCode::OK);
    VSegmentView view;
    EXPECT_FALSE(recovered.FindView(allocation.view.vsegment_id, &view));
    EXPECT_TRUE(recovered.Create("default"));
}

TEST(VSegmentManagerTest, ConcurrentProfileCreationSharesOneVSegment) {
    auto committer = Committer();
    committer->delay_create_begin = true;
    VSegmentManager manager(Config(), {Profile()}, committer);
    constexpr size_t kCallers = 8;
    std::barrier start(static_cast<std::ptrdiff_t>(kCallers));
    std::vector<VSegmentAllocationResult> results(kCallers);
    std::vector<std::thread> threads;
    for (size_t index = 0; index < kCallers; ++index) {
        threads.emplace_back([&, index] {
            start.arrive_and_wait();
            results[index] = manager.Create("default");
        });
    }
    for (auto& thread : threads) thread.join();
    for (const auto& result : results) {
        ASSERT_TRUE(result);
        EXPECT_EQ(result.view.vsegment_id, results[0].view.vsegment_id);
    }
    EXPECT_EQ(std::count(committer->mutations.begin(),
                         committer->mutations.end(),
                         "vsegment_create_begin"),
              1);
}

TEST(VSegmentManagerTest, AsyncCreationReturnsRetryableStatus) {
    auto committer = Committer();
    committer->delay_create_begin = true;
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto first = manager.RequestCreate("default", 25);
    EXPECT_EQ(first.error, ErrorCode::VSEGMENT_CREATING);
    EXPECT_NE(first.detail.find("retry_after_ms=25"), std::string::npos);

    VSegmentAllocationResult completed;
    for (int attempt = 0; attempt < 20; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        completed = manager.RequestCreate("default");
        if (completed.error != ErrorCode::VSEGMENT_CREATING) break;
    }
    ASSERT_TRUE(completed) << completed.detail;
    EXPECT_EQ(std::count(committer->mutations.begin(),
                         committer->mutations.end(),
                         "vsegment_create_begin"),
              1);
}

TEST(VSegmentManagerTest, PutStartCreatesThenReturnsIdempotentDescriptor) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto creating = manager.StartPut("put-1", 96);
    EXPECT_EQ(creating.error, ErrorCode::VSEGMENT_CREATING);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        started = manager.StartPut("put-1", 96);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(started.replica.partition_id, "partition-1");
    EXPECT_EQ(started.replica.logical_offset, 0);
    EXPECT_EQ(started.replica.length, 96);

    auto retry = manager.StartPut("put-1", 96);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry.replica.vsegment_id, started.replica.vsegment_id);
    EXPECT_EQ(retry.replica.logical_offset, started.replica.logical_offset);
}

TEST(VSegmentManagerTest, FencesStalePartitionOwnerRequests) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    ASSERT_EQ(manager.SetRouteEpoch(7), ErrorCode::OK);
    auto stale = manager.StartPut("put-1", 64, "default", 6);
    EXPECT_EQ(stale.error, ErrorCode::STALE_ROUTE);
    auto current = manager.StartPut("put-1", 64, "default", 7);
    EXPECT_EQ(current.error, ErrorCode::VSEGMENT_CREATING);
    EXPECT_EQ(manager.SetRouteEpoch(6), ErrorCode::STALE_ROUTE);
}

TEST(VSegmentManagerTest, ExposesLifecycleStatsAndGcsOperationTombstone) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(manager.ReservePut(allocation.view.vsegment_id, "put-1", 64));
    ASSERT_EQ(manager.AbortPut(allocation.view.vsegment_id, "put-1"),
              ErrorCode::OK);
    auto stats = manager.Stats();
    EXPECT_EQ(stats.active, 1);
    EXPECT_EQ(stats.reservations, 0);
    EXPECT_EQ(stats.committed_allocations, 0);
    EXPECT_EQ(manager.ForgetOperation("put-1"), ErrorCode::OK);
    EXPECT_EQ(manager.ForgetOperation("put-1"), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(VSegmentManagerTest, RetirementPersistenceFailureRollsBackExtent) {
    auto committer = Committer();
    VSegmentManager manager(Config(), {Profile()}, committer);
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    ASSERT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                         Lifecycle::DRAINING),
              ErrorCode::OK);

    committer->fail_next = true;
    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::PERSISTENT_FAIL);
    VSegmentView view;
    EXPECT_TRUE(manager.FindView(allocation.view.vsegment_id, &view));
    EXPECT_EQ(manager.Stats().draining, 1);

    EXPECT_EQ(manager.TransitionLifecycle(allocation.view.vsegment_id,
                                          Lifecycle::RETIRED),
              ErrorCode::OK);
    EXPECT_FALSE(manager.FindView(allocation.view.vsegment_id, &view));
}

TEST(VSegmentHaTest, ReplaysContinuousPartitionRevisions) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    auto next = base;
    next.metadata_revision = 11;
    VSegmentOpLogRecord record{"partition-1", 4, 11, "logical_reserve",
                               next};
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);

    PartitionVSegmentSnapshot recovered;
    std::string detail;
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered, &detail),
              ErrorCode::OK)
        << detail;
    EXPECT_EQ(recovered.metadata_revision, 11);
    EXPECT_EQ(recovered.route_epoch, 4);
}

TEST(VSegmentHaTest, RejectsRevisionGapAndStaleEpoch) {
    PartitionVSegmentSnapshot base;
    base.partition_id = "partition-1";
    base.config_generation = 1;
    base.route_epoch = 4;
    base.metadata_revision = 10;
    auto next = base;
    next.metadata_revision = 12;
    VSegmentOpLogRecord record{"partition-1", 4, 12, "logical_reserve",
                               next};
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    PartitionVSegmentSnapshot recovered;
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered),
              ErrorCode::OPLOG_ENTRY_NOT_FOUND);

    record.metadata_revision = 11;
    record.route_epoch = 3;
    record.state.metadata_revision = 11;
    record.state.route_epoch = 3;
    struct_json::to_json(record, entry.payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    EXPECT_EQ(ReplayVSegmentState(base, {entry}, &recovered),
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, OwnsPartitionPutAndViewLifecycle) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    EXPECT_EQ(service.CommitPut(started.replica, 8, "put-1", "object-1"),
              ErrorCode::OK);
    VSegmentView view;
    EXPECT_EQ(service.LoadView("partition-1", started.replica.vsegment_id,
                               &view),
              ErrorCode::OK);
    EXPECT_EQ(view.partition_id, "partition-1");
    EXPECT_EQ(service.StartPut("partition-1", 7, "put-2", 16).error,
              ErrorCode::STALE_ROUTE);
    EXPECT_EQ(service.RemovePartition("partition-1", 9), ErrorCode::OK);
    EXPECT_EQ(service.LoadView("partition-1", view.vsegment_id, &view),
              ErrorCode::STALE_ROUTE);
}

TEST(VSegmentServiceTest, RejectsDescriptorMismatchBeforeCommit) {
    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = 1;
    snapshot.policy_digest = "policy";
    snapshot.default_profile = "default";
    snapshot.profile_specs = {Profile()};
    snapshot.quotas = {
        {"partition-1", "default", "DRAM",
         {{"segment-a", 0, 512}, {"segment-b", 0, 512}}}};
    VSegmentService service(snapshot);
    ASSERT_EQ(service.AddPartition("partition-1", 8, Committer()),
              ErrorCode::OK);

    VSegmentPutStartResult started;
    for (int attempt = 0; attempt < 20; ++attempt) {
        started = service.StartPut("partition-1", 8, "put-1", 80);
        if (started.error != ErrorCode::VSEGMENT_CREATING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(started) << started.detail;
    auto mismatched = started.replica;
    ++mismatched.logical_offset;
    EXPECT_EQ(service.CommitPut(mismatched, 8, "put-1", "object-1"),
              ErrorCode::INVALID_VERSION);
    EXPECT_EQ(service.CommitPut(started.replica, 8, "put-1", "object-1"),
              ErrorCode::OK);
}

}  // namespace
}  // namespace mooncake::vsegment
