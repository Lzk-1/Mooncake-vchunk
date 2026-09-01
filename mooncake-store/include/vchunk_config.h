#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct VChunkConfig;

enum class VCSliceSizeLevel : uint32_t {
    k4K = 4U * 1024U,
    k64K = 64U * 1024U,
    k256K = 256U * 1024U,
    k1M = 1024U * 1024U,
};

enum class VChunkHAMode : uint8_t {
    DISABLED = 0,
    ACTIVE_ONLY = 1,
    SHADOW = 2,
    RECOVERABLE = 3,
};

enum class VChunkSlicePolicy : uint8_t {
    AUTO = 0,
    FIXED = 1,
};

tl::expected<VChunkHAMode, ErrorCode> ParseVChunkHAMode(
    const std::string& value);
tl::expected<VChunkSlicePolicy, ErrorCode> ParseVChunkSlicePolicy(
    const std::string& value);
tl::expected<VCSliceSizeLevel, ErrorCode> ParseVChunkSliceSize(
    const std::string& value);

constexpr uint32_t SliceSizeLevelToBytes(VCSliceSizeLevel level) {
    return static_cast<uint32_t>(level);
}

VCSliceSizeLevel SelectVChunkSliceSize(uint64_t value_size,
                                      bool is_ssd_segment);
VCSliceSizeLevel SelectVChunkSliceSize(uint64_t value_size,
                                      bool is_ssd_segment,
                                      const VChunkConfig& config);

struct VChunkConfig {
    bool enabled{false};
    uint64_t creating_timeout_ms{30'000};
    uint64_t releasing_timeout_ms{60'000};
    uint64_t recovering_timeout_ms{60'000};
    uint32_t max_slice_retry{3};
    uint32_t max_recovering_attempts{2};
    uint8_t replica_num{1};
    uint32_t max_replica_count{3};
    uint32_t max_slice_count{4096};
    uint64_t max_metadata_bytes{1024U * 1024U};
    uint32_t max_creating_objects{1024};
    uint64_t reaper_interval_ms{1000};
    uint32_t reaper_max_scan{128};
    bool enable_recovery{true};
    bool enable_read_merge{true};
    bool enable_replica_fallback{true};
    uint32_t max_concurrent_reads{0};
    uint64_t read_timeout_ms{10'000};
    bool etcd_incremental_update{true};
    bool enable_ha_recovery{false};
    VChunkHAMode ha_mode{VChunkHAMode::ACTIVE_ONLY};
    uint64_t allocator_claim_timeout_ms{30'000};
    bool verify_recovered_data{true};
    std::string submaster_id;
    uint64_t route_version{0};
    uint64_t owner_epoch{0};
    std::vector<std::string> static_slot_owners;
    bool enable_dynamic_membership{false};
    uint32_t membership_lease_ttl_sec{15};
    VChunkSlicePolicy slice_policy{VChunkSlicePolicy::AUTO};
    VCSliceSizeLevel fixed_slice_size{VCSliceSizeLevel::k64K};
    uint64_t slice_threshold_4k_to_64k{64U * 1024U};
    uint64_t slice_threshold_64k_to_256k{256U * 1024U};
    uint64_t slice_threshold_256k_to_1m{1024U * 1024U};
    uint32_t min_segments_per_replica{1};
    uint32_t max_segments_per_replica{0};
    uint32_t max_segments_per_vchunk{0};
    bool allow_segment_limit_fallback{true};
    uint32_t min_stripe_slices{1};
    uint32_t max_stripe_slices{256};
    uint32_t cleanup_max_attempts{8};
    uint64_t cleanup_retry_backoff_ms{100};
    uint32_t max_etcd_txn_ops{64};
    uint64_t max_etcd_txn_bytes{1024U * 1024U};

    ErrorCode Validate() const;

    YLT_REFL(VChunkConfig, enabled, creating_timeout_ms,
             releasing_timeout_ms, recovering_timeout_ms, max_slice_retry,
             max_recovering_attempts, replica_num, max_replica_count,
             max_slice_count,
             max_metadata_bytes, max_creating_objects, reaper_interval_ms,
             reaper_max_scan, enable_recovery, enable_read_merge,
             enable_replica_fallback, max_concurrent_reads, read_timeout_ms,
             etcd_incremental_update, enable_ha_recovery,
             ha_mode, allocator_claim_timeout_ms, verify_recovered_data,
             submaster_id, route_version, owner_epoch, static_slot_owners,
             enable_dynamic_membership, membership_lease_ttl_sec,
             slice_policy, fixed_slice_size, slice_threshold_4k_to_64k,
             slice_threshold_64k_to_256k, slice_threshold_256k_to_1m,
             min_segments_per_replica, max_segments_per_replica,
             max_segments_per_vchunk, allow_segment_limit_fallback,
             min_stripe_slices, max_stripe_slices, cleanup_max_attempts,
             cleanup_retry_backoff_ms, max_etcd_txn_ops,
             max_etcd_txn_bytes);
};

}  // namespace mooncake
