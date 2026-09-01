#include "vchunk_config.h"

namespace mooncake {

namespace {

bool IsKnownSliceSize(VCSliceSizeLevel level) {
    switch (level) {
        case VCSliceSizeLevel::k4K:
        case VCSliceSizeLevel::k64K:
        case VCSliceSizeLevel::k256K:
        case VCSliceSizeLevel::k1M:
            return true;
    }
    return false;
}

}  // namespace

tl::expected<VChunkHAMode, ErrorCode> ParseVChunkHAMode(
    const std::string& value) {
    if (value == "disabled") return VChunkHAMode::DISABLED;
    if (value == "active_only") return VChunkHAMode::ACTIVE_ONLY;
    if (value == "shadow") return VChunkHAMode::SHADOW;
    if (value == "recoverable") return VChunkHAMode::RECOVERABLE;
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

VCSliceSizeLevel SelectVChunkSliceSize(uint64_t value_size,
                                      bool is_ssd_segment) {
    return SelectVChunkSliceSize(value_size, is_ssd_segment, VChunkConfig{});
}

tl::expected<VChunkSlicePolicy, ErrorCode> ParseVChunkSlicePolicy(
    const std::string& value) {
    if (value == "auto") return VChunkSlicePolicy::AUTO;
    if (value == "fixed") return VChunkSlicePolicy::FIXED;
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

tl::expected<VCSliceSizeLevel, ErrorCode> ParseVChunkSliceSize(
    const std::string& value) {
    if (value == "4K") return VCSliceSizeLevel::k4K;
    if (value == "64K") return VCSliceSizeLevel::k64K;
    if (value == "256K") return VCSliceSizeLevel::k256K;
    if (value == "1M") return VCSliceSizeLevel::k1M;
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

VCSliceSizeLevel SelectVChunkSliceSize(uint64_t value_size,
                                      bool is_ssd_segment,
                                      const VChunkConfig& config) {
    if (config.slice_policy == VChunkSlicePolicy::FIXED) {
        return config.fixed_slice_size;
    }
    if (is_ssd_segment || value_size < config.slice_threshold_4k_to_64k) {
        return VCSliceSizeLevel::k4K;
    }
    if (value_size < config.slice_threshold_64k_to_256k) {
        return VCSliceSizeLevel::k64K;
    }
    if (value_size < config.slice_threshold_256k_to_1m) {
        return VCSliceSizeLevel::k256K;
    }
    return VCSliceSizeLevel::k1M;
}

ErrorCode VChunkConfig::Validate() const {
    if (creating_timeout_ms == 0 || releasing_timeout_ms == 0 ||
        recovering_timeout_ms == 0 || max_recovering_attempts == 0 ||
        replica_num == 0 || replica_num > max_replica_count ||
        max_replica_count == 0 || max_slice_count == 0 ||
        max_metadata_bytes == 0 || max_creating_objects == 0 ||
        reaper_interval_ms == 0 || reaper_max_scan == 0 ||
        read_timeout_ms == 0 || allocator_claim_timeout_ms == 0 ||
        membership_lease_ttl_sec < 3 || cleanup_max_attempts == 0 ||
        cleanup_retry_backoff_ms == 0 || max_etcd_txn_ops < 4 ||
        max_etcd_txn_bytes == 0 || placement_metrics_ttl_ms == 0 ||
        placement_min_samples == 0 || placement_ewma_alpha <= 0 ||
        placement_ewma_alpha > 1 || min_segments_per_replica == 0 ||
        min_stripe_slices == 0 || max_stripe_slices < min_stripe_slices ||
        !IsKnownSliceSize(fixed_slice_size) ||
        slice_threshold_4k_to_64k == 0 ||
        slice_threshold_4k_to_64k >= slice_threshold_64k_to_256k ||
        slice_threshold_64k_to_256k >= slice_threshold_256k_to_1m ||
        static_cast<uint8_t>(slice_policy) >
            static_cast<uint8_t>(VChunkSlicePolicy::FIXED)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if ((max_segments_per_replica != 0 &&
         max_segments_per_replica < min_segments_per_replica) ||
        (max_segments_per_vchunk != 0 &&
         max_segments_per_vchunk < replica_num)) {
        return ErrorCode::INVALID_PARAMS;
    }
    const bool routing_disabled = submaster_id.empty() && route_version == 0 &&
                                  owner_epoch == 0 &&
                                  static_slot_owners.empty();
    if (!routing_disabled) {
        if (submaster_id.empty() || route_version == 0 || owner_epoch == 0 ||
            static_slot_owners.empty()) {
            return ErrorCode::INVALID_PARAMS;
        }
        for (const auto& owner : static_slot_owners) {
            if (owner.empty()) return ErrorCode::INVALID_PARAMS;
        }
    }
    if (enable_dynamic_membership && routing_disabled) {
        return ErrorCode::INVALID_PARAMS;
    }
    switch (ha_mode) {
        case VChunkHAMode::DISABLED:
        case VChunkHAMode::ACTIVE_ONLY:
        case VChunkHAMode::SHADOW:
        case VChunkHAMode::RECOVERABLE:
            break;
        default:
            return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake
