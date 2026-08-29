#include "vchunk_routing.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(VChunkRoutingTest, ResolvesStableUniqueOwner) {
    VChunkStaticRouteTable routes(9, {"submaster-a", "submaster-b"}, 3);
    ASSERT_EQ(routes.Validate(), ErrorCode::OK);
    auto first = routes.Resolve("tenant", "key");
    auto second = routes.Resolve("tenant", "key");
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->slot, second->slot);
    EXPECT_EQ(first->owner_submaster_id, second->owner_submaster_id);
    EXPECT_EQ(routes.CheckOwner(first->owner_submaster_id, "tenant", "key", 9,
                                3),
              ErrorCode::OK);
}

TEST(VChunkRoutingTest, RejectsWrongOwnerAndStaleRoute) {
    VChunkStaticRouteTable routes(9, {"submaster-a"}, 3);
    EXPECT_EQ(routes.CheckOwner("submaster-b", "tenant", "key", 9, 3),
              ErrorCode::NOT_OWNER);
    EXPECT_EQ(routes.CheckOwner("submaster-a", "tenant", "key", 8, 3),
              ErrorCode::ROUTE_CHANGED);
    EXPECT_EQ(routes.CheckOwner("submaster-a", "tenant", "key", 9, 2),
              ErrorCode::STALE_EPOCH);
}

TEST(VChunkRoutingTest, TransfersSlotWithMonotonicVersionAndEpoch) {
    VChunkDynamicRouteTable routes;
    VChunkRouteSnapshot snapshot;
    snapshot.route_version = 1;
    snapshot.slots.push_back(
        {0, VChunkSlotState::OWNED, "submaster-a", "", 5});
    ASSERT_EQ(routes.ApplySnapshot(std::move(snapshot)), ErrorCode::OK);
    ASSERT_EQ(routes.BeginTransfer(0, "submaster-b", 2), ErrorCode::OK);
    EXPECT_EQ(routes.Resolve(0)->state, VChunkSlotState::DRAINING);
    ASSERT_EQ(routes.MarkTransferring(0, 3), ErrorCode::OK);
    EXPECT_EQ(routes.CompleteTransfer(0, 5, 4), ErrorCode::STALE_EPOCH);
    ASSERT_EQ(routes.CompleteTransfer(0, 6, 4), ErrorCode::OK);
    auto route = routes.Resolve(0);
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->state, VChunkSlotState::OWNED);
    EXPECT_EQ(route->owner_submaster_id, "submaster-b");
    EXPECT_EQ(routes.Version(), 4U);
}

TEST(VChunkRoutingTest, RouteStorePublishesWithVersionCas) {
    InMemoryVChunkRouteStore store;
    VChunkRouteSnapshot first;
    first.route_version = 1;
    first.slots.push_back(
        {0, VChunkSlotState::OWNED, "submaster-a", "", 5});
    ASSERT_EQ(store.Publish(0, first), ErrorCode::OK);

    auto loaded = store.Load();
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->route_version, 1U);
    EXPECT_EQ(loaded->slots[0].owner_submaster_id, "submaster-a");

    auto second = first;
    second.route_version = 2;
    second.slots[0].state = VChunkSlotState::DRAINING;
    second.slots[0].target_submaster_id = "submaster-b";
    EXPECT_EQ(store.Publish(0, second), ErrorCode::ROUTE_CHANGED);
    EXPECT_EQ(store.Publish(1, second), ErrorCode::OK);
    EXPECT_EQ(store.Load()->route_version, 2U);
}

TEST(VChunkRoutingTest, AbortsOnlyBeforeOwnershipSideEffects) {
    VChunkDynamicRouteTable routes;
    VChunkRouteSnapshot snapshot;
    snapshot.route_version = 1;
    snapshot.slots.push_back(
        {0, VChunkSlotState::OWNED, "submaster-a", "", 5});
    ASSERT_EQ(routes.ApplySnapshot(std::move(snapshot)), ErrorCode::OK);
    ASSERT_EQ(routes.BeginTransfer(0, "submaster-b", 2), ErrorCode::OK);
    ASSERT_EQ(routes.AbortTransfer(0, 3), ErrorCode::OK);
    auto restored = routes.Resolve(0);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->state, VChunkSlotState::OWNED);
    EXPECT_TRUE(restored->target_submaster_id.empty());
    EXPECT_EQ(routes.Version(), 3U);

    ASSERT_EQ(routes.BeginTransfer(0, "submaster-b", 4), ErrorCode::OK);
    ASSERT_EQ(routes.MarkTransferring(0, 5), ErrorCode::OK);
    EXPECT_EQ(routes.AbortTransfer(0, 6),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

}  // namespace
}  // namespace mooncake
