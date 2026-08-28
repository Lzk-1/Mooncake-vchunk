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

}  // namespace
}  // namespace mooncake
