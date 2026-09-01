#include "vchunk_config.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(VChunkConfigTest, DefaultsAreSafeForProductionOptIn) {
    VChunkConfig config;
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.Validate(), ErrorCode::OK);
    EXPECT_EQ(config.max_slice_count, 4096U);
    EXPECT_EQ(config.max_metadata_bytes, 1024U * 1024U);
    EXPECT_EQ(config.max_creating_objects, 1024U);
    EXPECT_EQ(config.reaper_interval_ms, 1000U);
    EXPECT_EQ(config.reaper_max_scan, 128U);
    EXPECT_EQ(config.recovering_timeout_ms, 60'000U);
    EXPECT_EQ(config.max_recovering_attempts, 2U);
    EXPECT_EQ(config.replica_num, 1U);
    EXPECT_EQ(config.max_replica_count, 3U);
    EXPECT_TRUE(config.enable_recovery);
    EXPECT_TRUE(config.enable_read_merge);
    EXPECT_TRUE(config.enable_replica_fallback);
    EXPECT_EQ(config.read_timeout_ms, 10'000U);
    EXPECT_FALSE(config.enable_ha_recovery);
}

TEST(VChunkConfigTest, RejectsInvalidLimits) {
    VChunkConfig config;
    config.max_slice_count = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.max_metadata_bytes = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.creating_timeout_ms = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.max_creating_objects = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.reaper_interval_ms = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.recovering_timeout_ms = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.max_replica_count = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.replica_num = config.max_replica_count + 1;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.read_timeout_ms = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.submaster_id = "submaster-a";
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkConfigTest, ValidatesPlacementAndSlicePolicy) {
    VChunkConfig config;
    config.slice_policy = VChunkSlicePolicy::FIXED;
    config.fixed_slice_size = VCSliceSizeLevel::k256K;
    config.min_segments_per_replica = 2;
    config.max_segments_per_replica = 4;
    config.max_segments_per_vchunk = 4;
    EXPECT_EQ(config.Validate(), ErrorCode::OK);

    config.max_segments_per_replica = 1;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);
    config.max_segments_per_replica = 4;
    config.slice_threshold_64k_to_256k =
        config.slice_threshold_4k_to_64k;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkConfigTest, SelectsMemorySliceBoundaries) {
    EXPECT_EQ(SelectVChunkSliceSize(0, false), VCSliceSizeLevel::k4K);
    EXPECT_EQ(SelectVChunkSliceSize(64U * 1024U - 1, false),
              VCSliceSizeLevel::k4K);
    EXPECT_EQ(SelectVChunkSliceSize(64U * 1024U, false),
              VCSliceSizeLevel::k64K);
    EXPECT_EQ(SelectVChunkSliceSize(256U * 1024U - 1, false),
              VCSliceSizeLevel::k64K);
    EXPECT_EQ(SelectVChunkSliceSize(256U * 1024U, false),
              VCSliceSizeLevel::k256K);
    EXPECT_EQ(SelectVChunkSliceSize(1024U * 1024U - 1, false),
              VCSliceSizeLevel::k256K);
    EXPECT_EQ(SelectVChunkSliceSize(1024U * 1024U, false),
              VCSliceSizeLevel::k1M);
}

TEST(VChunkConfigTest, DynamicMembershipRequiresRoutingAndLeaseBudget) {
    VChunkConfig config;
    config.enable_dynamic_membership = true;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config.submaster_id = "submaster-a";
    config.route_version = 1;
    config.owner_epoch = 1;
    config.static_slot_owners = {"submaster-a"};
    EXPECT_EQ(config.Validate(), ErrorCode::OK);
    config.membership_lease_ttl_sec = 2;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkConfigTest, SsdAlwaysUsesFourKiB) {
    EXPECT_EQ(SelectVChunkSliceSize(8U * 1024U * 1024U, true),
              VCSliceSizeLevel::k4K);
}

TEST(VChunkConfigTest, ParsesExplicitHaModes) {
    EXPECT_EQ(*ParseVChunkHAMode("disabled"), VChunkHAMode::DISABLED);
    EXPECT_EQ(*ParseVChunkHAMode("active_only"),
              VChunkHAMode::ACTIVE_ONLY);
    EXPECT_EQ(*ParseVChunkHAMode("shadow"), VChunkHAMode::SHADOW);
    EXPECT_EQ(*ParseVChunkHAMode("recoverable"),
              VChunkHAMode::RECOVERABLE);
    EXPECT_EQ(ParseVChunkHAMode("unsafe").error(),
              ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake
