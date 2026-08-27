#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "vchunk_metadata.h"

namespace mooncake {

inline constexpr uint32_t kVChunkHAEventSchemaVersion = 1;
inline constexpr uint32_t kVChunkSnapshotSchemaVersion = 1;

enum class VChunkHAEventType : uint8_t {
    CREATE = 0,
    ACTIVATE = 1,
    REVOKE = 2,
    BEGIN_RELEASE = 3,
    RELEASED = 4,
    SLICE_UPDATE = 5,
    CLAIM_EPOCH = 6,
};

struct VChunkHAEvent {
    uint32_t schema_version{kVChunkHAEventSchemaVersion};
    VChunkHAEventType type{VChunkHAEventType::CREATE};
    uint64_t sequence_id{0};
    uint64_t leader_epoch{0};
    VChunkMetadataRecord record;

    YLT_REFL(VChunkHAEvent, schema_version, type, sequence_id, leader_epoch,
             record);
};

struct VChunkSnapshot {
    uint32_t schema_version{kVChunkSnapshotSchemaVersion};
    uint64_t last_sequence_id{0};
    uint64_t leader_epoch{0};
    std::vector<VChunkMetadataRecord> records;

    YLT_REFL(VChunkSnapshot, schema_version, last_sequence_id, leader_epoch,
             records);
};

using VChunkRecoveryEntries =
    std::unordered_map<std::string, VChunkMetadataRecord>;

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkHAEvent(
    const VChunkHAEvent& event, const VChunkConfig& config);
tl::expected<VChunkHAEvent, ErrorCode> DeserializeVChunkHAEvent(
    const std::vector<char>& bytes, const VChunkConfig& config);
tl::expected<std::vector<char>, ErrorCode> SerializeVChunkSnapshot(
    const VChunkSnapshot& snapshot, const VChunkConfig& config);
tl::expected<VChunkSnapshot, ErrorCode> DeserializeVChunkSnapshot(
    const std::vector<char>& bytes, const VChunkConfig& config);
ErrorCode ApplyVChunkHAEvent(const VChunkHAEvent& event,
                             VChunkRecoveryEntries& entries,
                             uint64_t& applied_sequence_id,
                             const VChunkConfig& config,
                             bool allow_global_sequence_gaps = false);

}  // namespace mooncake
