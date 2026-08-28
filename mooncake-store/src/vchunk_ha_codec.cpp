#include "vchunk_ha_codec.h"

#include <limits>

namespace mooncake {
namespace {

bool IsKnownEventType(VChunkHAEventType type) {
    return static_cast<uint8_t>(type) <=
           static_cast<uint8_t>(VChunkHAEventType::CLAIM_EPOCH);
}

std::string ScopedKey(const VChunkMetadataRecord& record) {
    std::string key = record.tenant_id;
    key.push_back('\0');
    key += record.key;
    return key;
}

size_t SnapshotLimit(const VChunkConfig& config) {
    if (config.max_metadata_bytes >
        std::numeric_limits<size_t>::max() / config.max_creating_objects) {
        return std::numeric_limits<size_t>::max();
    }
    return static_cast<size_t>(config.max_metadata_bytes) *
           config.max_creating_objects;
}

}  // namespace

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkHAEvent(
    const VChunkHAEvent& event, const VChunkConfig& config) {
    if (event.schema_version != kVChunkHAEventSchemaVersion ||
        !IsKnownEventType(event.type) ||
        ValidateVChunkMetadata(event.record, config) != ErrorCode::OK ||
        event.leader_epoch != event.record.leader_epoch) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto bytes = struct_pack::serialize(event);
    if (bytes.size() > config.max_metadata_bytes) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }
    return bytes;
}

tl::expected<VChunkHAEvent, ErrorCode> DeserializeVChunkHAEvent(
    const std::vector<char>& bytes, const VChunkConfig& config) {
    if (bytes.empty() || bytes.size() > config.max_metadata_bytes) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    VChunkHAEvent event;
    if (struct_pack::deserialize_to(event, bytes) != struct_pack::errc::ok) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto checked = SerializeVChunkHAEvent(event, config);
    if (!checked) return tl::make_unexpected(checked.error());
    return event;
}

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkSnapshot(
    const VChunkSnapshot& snapshot, const VChunkConfig& config) {
    if (snapshot.schema_version != kVChunkSnapshotSchemaVersion ||
        config.Validate() != ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (const auto& record : snapshot.records) {
        if (ValidateVChunkMetadata(record, config) != ErrorCode::OK ||
            record.leader_epoch > snapshot.leader_epoch) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    auto bytes = struct_pack::serialize(snapshot);
    if (bytes.size() > SnapshotLimit(config)) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }
    return bytes;
}

tl::expected<VChunkSnapshot, ErrorCode> DeserializeVChunkSnapshot(
    const std::vector<char>& bytes, const VChunkConfig& config) {
    if (bytes.empty() || bytes.size() > SnapshotLimit(config)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    VChunkSnapshot snapshot;
    if (struct_pack::deserialize_to(snapshot, bytes) !=
        struct_pack::errc::ok) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto checked = SerializeVChunkSnapshot(snapshot, config);
    if (!checked) return tl::make_unexpected(checked.error());
    return snapshot;
}

ErrorCode ApplyVChunkHAEvent(const VChunkHAEvent& event,
                             VChunkRecoveryEntries& entries,
                             uint64_t& applied_sequence_id,
                             const VChunkConfig& config,
                             bool allow_global_sequence_gaps) {
    if (event.sequence_id == 0) return ErrorCode::INVALID_PARAMS;
    auto encoded = SerializeVChunkHAEvent(event, config);
    if (!encoded) return encoded.error();
    if (event.sequence_id <= applied_sequence_id) return ErrorCode::OK;
    if (!allow_global_sequence_gaps &&
        event.sequence_id != applied_sequence_id + 1) {
        return ErrorCode::INVALID_VERSION;
    }
    const auto key = ScopedKey(event.record);
    const auto it = entries.find(key);
    if (event.type == VChunkHAEventType::REVOKE ||
        event.type == VChunkHAEventType::RELEASED) {
        if (it != entries.end() &&
            event.record.metadata_version < it->second.metadata_version) {
            return ErrorCode::INVALID_VERSION;
        }
        entries.erase(key);
    } else {
        if (it != entries.end() &&
            event.record.metadata_version < it->second.metadata_version) {
            return ErrorCode::INVALID_VERSION;
        }
        entries[key] = event.record;
    }
    applied_sequence_id = event.sequence_id;
    return ErrorCode::OK;
}

}  // namespace mooncake
