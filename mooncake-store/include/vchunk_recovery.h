#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "vchunk_config.h"
#include "vchunk_metadata.h"

namespace mooncake {

class AllocatorManager;

enum class VChunkRecoveryPhase : uint8_t {
    PROMOTION_PREPARE = 0,
    METADATA_REPLAY = 1,
    RESOURCE_CLAIMING = 2,
    DATA_VERIFYING = 3,
    INCOMPLETE_RECOVERING = 4,
    PUBLISHING = 5,
    ACTIVE_SERVING = 6,
    FAILED = 7,
};

struct VChunkRecoveredEntry {
    VChunkMetadataRecord record;
    std::vector<std::unique_ptr<AllocatedBuffer>> claims;

    VChunkRecoveredEntry() = default;
    VChunkRecoveredEntry(VChunkRecoveredEntry&&) noexcept = default;
    VChunkRecoveredEntry& operator=(VChunkRecoveredEntry&&) noexcept = default;
    VChunkRecoveredEntry(const VChunkRecoveredEntry&) = delete;
    VChunkRecoveredEntry& operator=(const VChunkRecoveredEntry&) = delete;
};

class VChunkRecoveryView {
   public:
    VChunkRecoveryView() = default;
    VChunkRecoveryView(VChunkRecoveryView&&) noexcept = default;
    VChunkRecoveryView& operator=(VChunkRecoveryView&&) noexcept = default;
    VChunkRecoveryView(const VChunkRecoveryView&) = delete;
    VChunkRecoveryView& operator=(const VChunkRecoveryView&) = delete;

    uint64_t leader_epoch{0};
    std::vector<VChunkRecoveredEntry> entries;
};

class VChunkRecoveryManager {
   public:
    explicit VChunkRecoveryManager(VChunkConfig config)
        : config_(std::move(config)) {}

    tl::expected<VChunkRecoveryView, ErrorCode> BuildIsolatedView(
        std::vector<VChunkMetadataRecord> records,
        const AllocatorManager& allocators, uint64_t leader_epoch);

    VChunkRecoveryPhase Phase() const { return phase_; }
    const std::string& FailureReason() const { return failure_reason_; }

   private:
    tl::expected<std::shared_ptr<BufferAllocatorBase>, ErrorCode> FindAllocator(
        const AllocatorManager& allocators,
        const VCSliceDescriptor& slice) const;
    tl::unexpected<ErrorCode> Fail(ErrorCode error, std::string reason);

    VChunkConfig config_;
    VChunkRecoveryPhase phase_{VChunkRecoveryPhase::PROMOTION_PREPARE};
    std::string failure_reason_;
};

}  // namespace mooncake
