#include "vchunk_master_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "allocation_strategy.h"
#include "vchunk_test_allocator.h"

namespace mooncake {
namespace {

using test::VChunkTestAllocator;

struct ManagerFixture {
    AllocatorManager allocators;
    std::shared_ptr<VChunkTestAllocator> first =
        std::make_shared<VChunkTestAllocator>("segment-a", 0x700000000ULL,
                                              1024U * 1024U);
    std::shared_ptr<VChunkTestAllocator> second =
        std::make_shared<VChunkTestAllocator>("segment-b", 0x800000000ULL,
                                              1024U * 1024U);

    ManagerFixture() {
        allocators.addAllocator("segment-a", first);
        allocators.addAllocator("segment-b", second);
    }
};

VChunkConfig EnabledConfig() {
    VChunkConfig config;
    config.enabled = true;
    return config;
}

TEST(VChunkMasterManagerTest, RunsPutGetRemoveLifecycle) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");

    auto created = manager.PutStart(fixture.allocators, tenant, "key", 10U * 1024U,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->status, VChunkStatus::CREATING);
    EXPECT_EQ(created->slice_count, 3U);
    EXPECT_EQ(created->row_size, 2U);
    EXPECT_FALSE(manager.Get(tenant, "key").has_value());

    EXPECT_EQ(manager.PutEnd(tenant, "key", "wrong-id", 200),
              ErrorCode::INVALID_VERSION);
    EXPECT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);
    EXPECT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    auto active = manager.Get(tenant, "key");
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(active->status, VChunkStatus::ACTIVE);
    for (const auto& slice : active->slices) {
        EXPECT_EQ(slice.status, VCSliceStatus::COMPLETED);
    }

    EXPECT_EQ(manager.Remove(tenant, "key", 300), ErrorCode::OK);
    EXPECT_EQ(manager.Remove(tenant, "key", 301), ErrorCode::OK);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
    EXPECT_EQ(fixture.first->size(), 0U);
    EXPECT_EQ(fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, RevokeIsIdempotentAndReleasesBuffers) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 8192,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    EXPECT_GT(fixture.first->size() + fixture.second->size(), 0U);

    EXPECT_EQ(manager.PutRevoke(tenant, "key", created->vchunk_id),
              ErrorCode::OK);
    EXPECT_EQ(manager.PutRevoke(tenant, "key", created->vchunk_id),
              ErrorCode::OK);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, PersistsConfiguredReplicaLayout) {
    ManagerFixture fixture;
    auto config = EnabledConfig();
    config.replica_num = 2;
    VChunkMasterManager manager(config);

    auto created = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                    "replicated", 8192, false, 100);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->replica_num, 2U);
    EXPECT_EQ(created->slice_count, 2U);
    ASSERT_EQ(created->slices.size(), 4U);
    for (size_t i = 0; i < created->slices.size(); ++i) {
        EXPECT_EQ(created->slices[i].slice_index, i % 2);
        EXPECT_EQ(created->slices[i].replica_index, i / 2);
        EXPECT_EQ(created->slices[i].replica_group_id, i % 2);
        EXPECT_EQ(created->slices[i].allocation_generation, 1U);
        EXPECT_FALSE(created->slices[i].segment_instance_id.empty());
    }
}

TEST(VChunkMasterManagerTest, FencesRequestsFromObsoleteLeaderEpoch) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    ASSERT_EQ(manager.ActivateLeaderEpoch(2), ErrorCode::OK);
    auto created = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                    "epoch-key", 4096, false, 100, {}, 2);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->leader_epoch, 2U);
    EXPECT_EQ(manager.PutEnd(TenantId("tenant"), "epoch-key",
                             created->vchunk_id, 200, 1),
              ErrorCode::STALE_EPOCH);

    ASSERT_EQ(manager.ActivateLeaderEpoch(3), ErrorCode::OK);
    EXPECT_EQ(manager.PutEnd(TenantId("tenant"), "epoch-key",
                             created->vchunk_id, 200, 2),
              ErrorCode::STALE_EPOCH);
    manager.DeactivateLeader();
    EXPECT_EQ(manager.PutRevoke(TenantId("tenant"), "epoch-key",
                                created->vchunk_id, 3),
              ErrorCode::NOT_LEADER);
    EXPECT_EQ(manager.Get(TenantId("tenant"), "epoch-key").error(),
              ErrorCode::NOT_LEADER);
}

TEST(VChunkMasterManagerTest, RejectsKeysOwnedByAnotherStaticSubmaster) {
    ManagerFixture fixture;
    auto config = EnabledConfig();
    config.submaster_id = "submaster-a";
    config.route_version = 1;
    config.owner_epoch = 1;
    config.static_slot_owners = {"submaster-b"};
    VChunkMasterManager manager(config);

    auto created = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                    "remote-key", 4096, false, 100);
    ASSERT_FALSE(created.has_value());
    EXPECT_EQ(created.error(), ErrorCode::NOT_OWNER);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, AppliesNewSubmasterOwnershipAtomically) {
    ManagerFixture fixture;
    auto config = EnabledConfig();
    config.submaster_id = "submaster-a";
    config.route_version = 1;
    config.owner_epoch = 1;
    config.static_slot_owners = {"submaster-a"};
    VChunkMasterManager manager(config);
    ASSERT_TRUE(manager.PutStart(fixture.allocators, TenantId("tenant"),
                                 "before-move", 4096, false, 100)
                    .has_value());

    VChunkRouteSnapshot snapshot;
    snapshot.route_version = 2;
    snapshot.slots.push_back(
        {0, VChunkSlotState::OWNED, "submaster-b", "", 2});
    ASSERT_EQ(manager.ApplyRouteSnapshot(std::move(snapshot)), ErrorCode::OK);
    auto rejected = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                     "after-move", 4096, false, 101);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::NOT_OWNER);
}

TEST(VChunkMasterManagerTest, PersistsRouteBeforeApplyingOwnership) {
    auto config = EnabledConfig();
    config.submaster_id = "submaster-a";
    config.route_version = 1;
    config.owner_epoch = 1;
    config.static_slot_owners = {"submaster-a"};
    auto routes = std::make_shared<InMemoryVChunkRouteStore>();
    VChunkMasterManager manager(config, nullptr, nullptr, routes);
    ASSERT_EQ(routes->Load()->route_version, 1U);

    VChunkRouteSnapshot snapshot;
    snapshot.route_version = 2;
    snapshot.slots.push_back(
        {0, VChunkSlotState::DRAINING, "submaster-a", "submaster-b", 1});
    ASSERT_EQ(manager.ApplyRouteSnapshot(snapshot), ErrorCode::OK);
    auto persisted = routes->Load();
    ASSERT_TRUE(persisted.has_value());
    EXPECT_EQ(persisted->route_version, 2U);
    EXPECT_EQ(persisted->slots[0].state, VChunkSlotState::DRAINING);

    EXPECT_EQ(manager.ApplyRouteSnapshot(std::move(snapshot)),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMasterManagerTest, RequiresDurabilityBeforePublishingState) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    std::vector<VChunkHAEventType> events;
    manager.SetDurabilitySink(
        [&](VChunkHAEventType type, const VChunkMetadataRecord&) {
            events.push_back(type);
            return ErrorCode::ETCD_OPERATION_ERROR;
        });

    auto failed = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                   "key", 4096, false, 100);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::ETCD_OPERATION_ERROR);
    ASSERT_EQ(events.size(), 1U);
    EXPECT_EQ(events[0], VChunkHAEventType::CREATE);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, ConcurrentPutStartPublishesOneObject) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    std::atomic<int> success{0};
    std::atomic<int> already_exists{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            auto result = manager.PutStart(fixture.allocators, tenant, "key",
                                           8192, false, 100);
            if (result.has_value()) {
                ++success;
            } else if (result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                ++already_exists;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(success.load(), 1);
    EXPECT_EQ(already_exists.load(), 7);
    EXPECT_EQ(manager.SizeForTesting(), 1U);
}

TEST(VChunkMasterManagerTest, IsolatesSameKeyAcrossTenants) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    auto first = manager.PutStart(fixture.allocators, TenantId("tenant-a"),
                                  "key", 4096, false, 100);
    auto second = manager.PutStart(fixture.allocators, TenantId("tenant-b"),
                                   "key", 4096, false, 100);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first->vchunk_id, second->vchunk_id);
    EXPECT_EQ(manager.SizeForTesting(), 2U);
}

TEST(VChunkMasterManagerTest, GetAndRemoveAreSerializedSafely) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 8192,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    ASSERT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    std::atomic<bool> stop{false};
    std::atomic<int> valid_reads{0};
    std::thread reader([&] {
        while (!stop.load()) {
            auto result = manager.Get(tenant, "key");
            if (result.has_value()) {
                EXPECT_EQ(result->status, VChunkStatus::ACTIVE);
                ++valid_reads;
            } else {
                EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
            }
        }
    });
    EXPECT_EQ(manager.Remove(tenant, "key", 300), ErrorCode::OK);
    stop.store(true);
    reader.join();

    EXPECT_EQ(manager.SizeForTesting(), 0U);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, ReadLeaseDefersBufferReleaseAfterRemove) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 8192,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    ASSERT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    const auto allocated = fixture.first->size() + fixture.second->size();
    ASSERT_GT(allocated, 0U);
    {
        auto read = manager.AcquireRead(tenant, "key");
        ASSERT_TRUE(read.has_value());
        EXPECT_EQ(manager.Remove(tenant, "key", 300), ErrorCode::OK);
        EXPECT_FALSE(manager.Get(tenant, "key").has_value());
        EXPECT_EQ(fixture.first->size() + fixture.second->size(), allocated);
    }
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, DisabledConfigurationRejectsCreation) {
    ManagerFixture fixture;
    VChunkMasterManager manager(VChunkConfig{});
    auto result = manager.PutStart(fixture.allocators, TenantId::Default(),
                                   "key", 4096, false, 100);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMasterManagerTest, PiercingVersionRejectsSsdSegments) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    auto result = manager.PutStart(fixture.allocators, TenantId::Default(),
                                   "key", 4096, true, 100);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, CreatingBacklogTripsAllocationCircuitBreaker) {
    ManagerFixture fixture;
    auto config = EnabledConfig();
    config.max_creating_objects = 1;
    VChunkMasterManager manager(config);
    ASSERT_TRUE(manager.PutStart(fixture.allocators, TenantId("tenant"),
                                 "first", 4096, false, 100)
                    .has_value());
    auto blocked = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                    "second", 4096, false, 101);
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

}  // namespace
}  // namespace mooncake
