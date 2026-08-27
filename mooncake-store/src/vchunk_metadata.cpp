#include "vchunk_metadata.h"

#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace mooncake {
namespace {

struct VCSliceDescriptorV1 {
    uint32_t slice_index{0};
    std::string target_segment_name;
    uint64_t target_offset{0};
    uint32_t logical_length{0};
    uint32_t allocated_length{0};
    VCSliceStatus status{VCSliceStatus::PENDING};
    uint32_t retry_count{0};

    YLT_REFL(VCSliceDescriptorV1, slice_index, target_segment_name,
             target_offset, logical_length, allocated_length, status,
             retry_count);
};

struct VChunkMetadataRecordV1 {
    uint32_t schema_version{1};
    std::string vchunk_id;
    std::string tenant_id;
    std::string key;
    uint64_t total_size{0};
    uint32_t slice_count{0};
    VCSliceSizeLevel slice_size_level{VCSliceSizeLevel::k4K};
    std::vector<VCSliceDescriptorV1> slices;
    uint32_t row_size{0};
    VChunkStatus status{VChunkStatus::CREATING};
    int64_t created_at_ms{0};
    int64_t last_updated_at_ms{0};

    YLT_REFL(VChunkMetadataRecordV1, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, slices, row_size,
             status, created_at_ms, last_updated_at_ms);
};

VChunkMetadataRecord UpgradeV1(VChunkMetadataRecordV1 legacy) {
    VChunkMetadataRecord record;
    record.vchunk_id = std::move(legacy.vchunk_id);
    record.tenant_id = std::move(legacy.tenant_id);
    record.key = std::move(legacy.key);
    record.total_size = legacy.total_size;
    record.slice_count = legacy.slice_count;
    record.slice_size_level = legacy.slice_size_level;
    record.row_size = legacy.row_size;
    record.status = legacy.status;
    record.created_at_ms = legacy.created_at_ms;
    record.last_updated_at_ms = legacy.last_updated_at_ms;
    record.slices.reserve(legacy.slices.size());
    for (auto& old_slice : legacy.slices) {
        VCSliceDescriptor slice;
        slice.slice_index = old_slice.slice_index;
        slice.target_segment_name = std::move(old_slice.target_segment_name);
        slice.target_offset = old_slice.target_offset;
        slice.logical_length = old_slice.logical_length;
        slice.allocated_length = old_slice.allocated_length;
        slice.status = old_slice.status;
        slice.retry_count = old_slice.retry_count;
        record.slices.push_back(std::move(slice));
    }
    return record;
}

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

bool IsKnownSliceStatus(VCSliceStatus status) {
    switch (status) {
        case VCSliceStatus::PENDING:
        case VCSliceStatus::COMPLETED:
        case VCSliceStatus::FAILED:
            return true;
    }
    return false;
}

bool IsKnownVChunkStatus(VChunkStatus status) {
    switch (status) {
        case VChunkStatus::CREATING:
        case VChunkStatus::ACTIVE:
        case VChunkStatus::RELEASING:
        case VChunkStatus::RELEASED:
        case VChunkStatus::FAILED:
        case VChunkStatus::RECOVERING:
        case VChunkStatus::ABANDONED:
            return true;
    }
    return false;
}

}  // namespace

ErrorCode ValidateVChunkMetadata(const VChunkMetadataRecord& record,
                                 const VChunkConfig& config) {
    if (config.Validate() != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.schema_version != kVChunkMetadataSchemaVersion) {
        return ErrorCode::INVALID_VERSION;
    }
    if (record.vchunk_id.empty() || record.tenant_id.empty() ||
        record.key.empty() || record.total_size == 0 ||
        !IsKnownSliceSize(record.slice_size_level) ||
        !IsKnownVChunkStatus(record.status)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.slice_count == 0 ||
        record.slice_count > config.max_slice_count ||
        record.slices.size() != record.slice_count || record.row_size == 0 ||
        record.row_size > record.slice_count || record.replica_num == 0 ||
        record.replica_num > config.max_replica_count ||
        record.created_at_ms < 0 ||
        record.last_updated_at_ms < record.created_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }

    const uint64_t slice_size =
        SliceSizeLevelToBytes(record.slice_size_level);
    uint64_t covered_bytes = 0;
    std::unordered_set<std::string> segments_in_row;
    segments_in_row.reserve(record.row_size);
    for (uint32_t i = 0; i < record.slice_count; ++i) {
        const auto& slice = record.slices[i];
        if (slice.slice_index != i || slice.target_segment_name.empty() ||
            slice.logical_length == 0 ||
            slice.logical_length > slice.allocated_length ||
            slice.allocated_length < slice_size ||
            !IsKnownSliceStatus(slice.status) ||
            slice.retry_count > config.max_slice_retry ||
            slice.replica_index >= record.replica_num) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (slice.target_offset >
            std::numeric_limits<uint64_t>::max() - slice.allocated_length) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (covered_bytes >
            std::numeric_limits<uint64_t>::max() - slice.logical_length) {
            return ErrorCode::INVALID_PARAMS;
        }
        covered_bytes += slice.logical_length;

        if (i % record.row_size == 0) {
            segments_in_row.clear();
        }
        if (!segments_in_row.insert(slice.target_segment_name).second) {
            return ErrorCode::INVALID_PARAMS;
        }
    }

    if (covered_bytes != record.total_size) {
        return ErrorCode::INVALID_PARAMS;
    }
    for (const auto& group : record.slice_groups) {
        if (group.segment_name.empty() ||
            group.replica_index >= record.replica_num ||
            group.slice_indices.empty()) {
            return ErrorCode::INVALID_PARAMS;
        }
        for (const auto slice_index : group.slice_indices) {
            if (slice_index >= record.slice_count) {
                return ErrorCode::INVALID_PARAMS;
            }
        }
    }
    return ErrorCode::OK;
}

ErrorCode ValidateVChunkTransition(VChunkStatus from, VChunkStatus to) {
    if (!IsKnownVChunkStatus(from) || !IsKnownVChunkStatus(to)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (from == to) {
        return ErrorCode::OK;
    }
    switch (from) {
        case VChunkStatus::CREATING:
            return (to == VChunkStatus::ACTIVE ||
                    to == VChunkStatus::RECOVERING ||
                    to == VChunkStatus::FAILED ||
                    to == VChunkStatus::ABANDONED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::ACTIVE:
            return (to == VChunkStatus::RELEASING ||
                    to == VChunkStatus::RECOVERING ||
                    to == VChunkStatus::FAILED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::RELEASING:
            return (to == VChunkStatus::RELEASED ||
                    to == VChunkStatus::FAILED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::FAILED:
            return to == VChunkStatus::RELEASING ? ErrorCode::OK
                                                 : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::RECOVERING:
            return (to == VChunkStatus::ACTIVE || to == VChunkStatus::FAILED ||
                    to == VChunkStatus::ABANDONED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::ABANDONED:
            return to == VChunkStatus::RELEASING ? ErrorCode::OK
                                                 : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::RELEASED:
            return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::INVALID_PARAMS;
}

VChunkMetadataIndex BuildVChunkMetadataIndex(
    const VChunkMetadataRecord& record) {
    VChunkMetadataIndex index;
    index.schema_version = record.schema_version;
    index.vchunk_id = record.vchunk_id;
    index.tenant_id = record.tenant_id;
    index.key = record.key;
    index.total_size = record.total_size;
    index.slice_count = record.slice_count;
    index.slice_size_level = record.slice_size_level;
    index.row_size = record.row_size;
    index.status = record.status;
    index.created_at_ms = record.created_at_ms;
    index.last_updated_at_ms = record.last_updated_at_ms;
    index.replica_num = record.replica_num;
    index.leader_epoch = record.leader_epoch;
    index.metadata_version = record.metadata_version;
    index.slice_groups = record.slice_groups;
    return index;
}

std::vector<VCSlicePartition> PartitionVChunkSlices(
    const VChunkMetadataRecord& record) {
    std::vector<VCSlicePartition> partitions;
    std::unordered_map<std::string, size_t> positions;
    for (const auto& slice : record.slices) {
        auto [it, inserted] =
            positions.emplace(slice.target_segment_name, partitions.size());
        if (inserted) {
            partitions.push_back(VCSlicePartition{slice.target_segment_name,
                                                  {}});
        }
        partitions[it->second].slices.push_back(slice);
    }
    return partitions;
}

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkMetadata(
    const VChunkMetadataRecord& record, const VChunkConfig& config) {
    const auto validation = ValidateVChunkMetadata(record, config);
    if (validation != ErrorCode::OK) {
        return tl::make_unexpected(validation);
    }
    auto bytes = struct_pack::serialize(record);
    if (bytes.size() > config.max_metadata_bytes) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }
    return bytes;
}

tl::expected<VChunkMetadataRecord, ErrorCode> DeserializeVChunkMetadata(
    const std::vector<char>& bytes, const VChunkConfig& config) {
    if (config.Validate() != ErrorCode::OK || bytes.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (bytes.size() > config.max_metadata_bytes) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

    VChunkMetadataRecord record;
    const auto current_result = struct_pack::deserialize_to(record, bytes);
    if (current_result != struct_pack::errc::ok ||
        record.schema_version != kVChunkMetadataSchemaVersion) {
        VChunkMetadataRecordV1 legacy;
        if (struct_pack::deserialize_to(legacy, bytes) !=
                struct_pack::errc::ok ||
            legacy.schema_version != 1) {
            return tl::make_unexpected(
                current_result == struct_pack::errc::ok
                    ? ErrorCode::INVALID_VERSION
                    : ErrorCode::INVALID_PARAMS);
        }
        record = UpgradeV1(std::move(legacy));
    }
    const auto validation = ValidateVChunkMetadata(record, config);
    if (validation != ErrorCode::OK) {
        return tl::make_unexpected(validation);
    }
    return record;
}

VChunkMetadata::VChunkMetadata(VChunkMetadataRecord record)
    : record_(std::move(record)) {}

VChunkMetadataRecord VChunkMetadata::Snapshot() const {
    SpinLocker guard(&lock_);
    return record_;
}

ErrorCode VChunkMetadata::TransitionTo(VChunkStatus next,
                                       int64_t updated_at_ms) {
    SpinLocker guard(&lock_);
    const auto validation = ValidateVChunkTransition(record_.status, next);
    if (validation != ErrorCode::OK ||
        updated_at_ms < record_.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    record_.status = next;
    record_.last_updated_at_ms = updated_at_ms;
    return ErrorCode::OK;
}

}  // namespace mooncake
