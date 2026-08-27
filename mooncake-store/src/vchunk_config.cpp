#include "vchunk_config.h"

namespace mooncake {

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
    if (is_ssd_segment || value_size < 64U * 1024U) {
        return VCSliceSizeLevel::k4K;
    }
    if (value_size < 256U * 1024U) {
        return VCSliceSizeLevel::k64K;
    }
    if (value_size < 1024U * 1024U) {
        return VCSliceSizeLevel::k256K;
    }
    return VCSliceSizeLevel::k1M;
}

ErrorCode VChunkConfig::Validate() const {
    if (creating_timeout_ms == 0 || releasing_timeout_ms == 0 ||
        recovering_timeout_ms == 0 || max_recovering_attempts == 0 ||
        max_replica_count == 0 || max_slice_count == 0 ||
        max_metadata_bytes == 0 || max_creating_objects == 0 ||
        reaper_interval_ms == 0 || reaper_max_scan == 0 ||
        read_timeout_ms == 0 || allocator_claim_timeout_ms == 0) {
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
