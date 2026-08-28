#include "vchunk_control_plane.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

class RouteTestControlPlane final : public VChunkControlPlane {
   public:
    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId&, const std::string&, uint64_t, int64_t) override {
        ++calls;
        if (error != ErrorCode::OK) return tl::make_unexpected(error);
        VChunkMetadataRecord record;
        record.vchunk_id = id;
        return record;
    }
    ErrorCode PutEnd(const TenantId&, const std::string&, const std::string&,
                     int64_t, uint64_t,
                     const std::vector<uint64_t>&) override {
        ++calls;
        return error;
    }
    ErrorCode PutRevoke(const TenantId&, const std::string&,
                        const std::string&, uint64_t) override {
        ++calls;
        return error;
    }
    tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId&, const std::string&) override {
        ++calls;
        if (error != ErrorCode::OK) return tl::make_unexpected(error);
        VChunkMetadataRecord record;
        record.vchunk_id = id;
        return VChunkControlPlaneRead{std::move(record), {}};
    }
    ErrorCode Remove(const TenantId&, const std::string&, int64_t,
                     uint64_t) override {
        ++calls;
        return error;
    }

    ErrorCode error{ErrorCode::OK};
    std::string id;
    uint32_t calls{0};
};

TEST(VChunkControlPlaneTest, RefreshesRouteAndRetriesOnOwnershipChange) {
    RouteTestControlPlane stale;
    stale.error = ErrorCode::NOT_OWNER;
    RouteTestControlPlane current;
    current.id = "current-owner";
    VChunkControlPlane* selected = &stale;
    uint32_t refreshes = 0;
    RefreshingVChunkControlPlane routed(
        [&](const auto&, const auto&)
            -> tl::expected<VChunkControlPlane*, ErrorCode> {
            return selected;
        },
        [&] {
            ++refreshes;
            selected = &current;
            return ErrorCode::OK;
        });

    auto result = routed.Get(TenantId("tenant"), "key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->record.vchunk_id, "current-owner");
    EXPECT_EQ(stale.calls, 1U);
    EXPECT_EQ(current.calls, 1U);
    EXPECT_EQ(refreshes, 1U);
}

TEST(VChunkControlPlaneTest, BoundsRouteRefreshRetries) {
    RouteTestControlPlane stale;
    stale.error = ErrorCode::ROUTE_CHANGED;
    uint32_t refreshes = 0;
    RefreshingVChunkControlPlane routed(
        [&](const auto&, const auto&)
            -> tl::expected<VChunkControlPlane*, ErrorCode> {
            return &stale;
        },
        [&] {
            ++refreshes;
            return ErrorCode::OK;
        },
        2);

    auto result = routed.PutStart(TenantId("tenant"), "key", 4096, 10);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::ROUTE_CHANGED);
    EXPECT_EQ(stale.calls, 3U);
    EXPECT_EQ(refreshes, 2U);
}

TEST(VChunkControlPlaneTest, DoesNotRetryApplicationErrors) {
    RouteTestControlPlane target;
    target.error = ErrorCode::OBJECT_ALREADY_EXISTS;
    uint32_t refreshes = 0;
    RefreshingVChunkControlPlane routed(
        [&](const auto&, const auto&)
            -> tl::expected<VChunkControlPlane*, ErrorCode> {
            return &target;
        },
        [&] {
            ++refreshes;
            return ErrorCode::OK;
        });
    auto result = routed.Get(TenantId("tenant"), "key");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(refreshes, 0U);
}

TEST(VChunkControlPlaneTest, DirectoryRefreshesPersistedOwnerView) {
    auto routes = std::make_shared<InMemoryVChunkRouteStore>();
    VChunkRouteSnapshot first;
    first.route_version = 1;
    first.slots.push_back(
        {0, VChunkSlotState::OWNED, "submaster-a", "", 1});
    ASSERT_EQ(routes->Publish(0, first), ErrorCode::OK);

    RouteTestControlPlane owner_a;
    RouteTestControlPlane owner_b;
    VChunkControlPlaneDirectory directory(routes);
    directory.SetTarget("submaster-a", &owner_a);
    directory.SetTarget("submaster-b", &owner_b);
    ASSERT_EQ(directory.Refresh(), ErrorCode::OK);
    EXPECT_EQ(*directory.Resolve(TenantId("tenant"), "key"), &owner_a);

    auto second = first;
    second.route_version = 2;
    second.slots[0].owner_submaster_id = "submaster-b";
    second.slots[0].owner_epoch = 2;
    ASSERT_EQ(routes->Publish(1, second), ErrorCode::OK);
    ASSERT_EQ(directory.Refresh(), ErrorCode::OK);
    EXPECT_EQ(directory.RouteVersion(), 2U);
    EXPECT_EQ(*directory.Resolve(TenantId("tenant"), "key"), &owner_b);
}

}  // namespace
}  // namespace mooncake
