#pragma once

#include <cstdint>

#include "types.h"

namespace mooncake {

enum class VCSliceSizeLevel : uint32_t {
    k4K = 4U * 1024U,
    k64K = 64U * 1024U,
    k256K = 256U * 1024U,
    k1M = 1024U * 1024U,
};

constexpr uint32_t SliceSizeLevelToBytes(VCSliceSizeLevel level) {
    return static_cast<uint32_t>(level);
}

VCSliceSizeLevel SelectVChunkSliceSize(uint64_t value_size,
                                      bool is_ssd_segment);

struct VChunkConfig {
    bool enabled{false};
    uint64_t creating_timeout_ms{30'000};
    uint64_t releasing_timeout_ms{60'000};
    uint64_t recovering_timeout_ms{60'000};
    uint32_t max_slice_retry{3};
    uint32_t max_recovering_attempts{2};
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
    uint64_t allocator_claim_timeout_ms{30'000};
    bool verify_recovered_data{true};

    ErrorCode Validate() const;

    YLT_REFL(VChunkConfig, enabled, creating_timeout_ms,
             releasing_timeout_ms, recovering_timeout_ms, max_slice_retry,
             max_recovering_attempts, max_replica_count, max_slice_count,
             max_metadata_bytes, max_creating_objects, reaper_interval_ms,
             reaper_max_scan, enable_recovery, enable_read_merge,
             enable_replica_fallback, max_concurrent_reads, read_timeout_ms,
             etcd_incremental_update, enable_ha_recovery,
             allocator_claim_timeout_ms, verify_recovered_data);
};

}  // namespace mooncake
