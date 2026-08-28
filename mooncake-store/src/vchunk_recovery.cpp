#include "vchunk_recovery.h"

#include <algorithm>
#include <limits>
#include <tuple>

#include "allocation_strategy.h"

namespace mooncake {
namespace {

struct ClaimTask {
    size_t record_index;
    size_t slice_index;
    const VCSliceDescriptor* slice;
};

}  // namespace

tl::unexpected<ErrorCode> VChunkRecoveryManager::Fail(ErrorCode error,
                                                       std::string reason) {
    phase_ = VChunkRecoveryPhase::FAILED;
    failure_reason_ = std::move(reason);
    return tl::unexpected(error);
}

tl::expected<std::shared_ptr<BufferAllocatorBase>, ErrorCode>
VChunkRecoveryManager::FindAllocator(const AllocatorManager& allocators,
                                     const VCSliceDescriptor& slice) const {
    const auto* candidates =
        allocators.getAllocators(slice.target_segment_name);
    if (!candidates) return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    for (const auto& allocator : *candidates) {
        if (allocator && allocator->getSegmentInstanceId() ==
                             slice.segment_instance_id) {
            return allocator;
        }
    }
    return tl::unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<VChunkRecoveryView, ErrorCode>
VChunkRecoveryManager::BuildIsolatedView(
    std::vector<VChunkMetadataRecord> records,
    const AllocatorManager& allocators, uint64_t leader_epoch) {
    phase_ = VChunkRecoveryPhase::METADATA_REPLAY;
    failure_reason_.clear();
    if (leader_epoch == 0 || config_.Validate() != ErrorCode::OK) {
        return Fail(ErrorCode::INVALID_PARAMS, "invalid recovery parameters");
    }

    std::vector<ClaimTask> tasks;
    for (size_t record_index = 0; record_index < records.size();
         ++record_index) {
        auto& record = records[record_index];
        if (ValidateVChunkMetadata(record, config_) != ErrorCode::OK) {
            return Fail(ErrorCode::INVALID_PARAMS, "invalid metadata");
        }
        if (record.leader_epoch >= leader_epoch) {
            return Fail(ErrorCode::STALE_EPOCH, "non-monotonic leader epoch");
        }
        for (size_t slice_index = 0; slice_index < record.slices.size();
             ++slice_index) {
            const auto& slice = record.slices[slice_index];
            if (slice.segment_instance_id.empty()) {
                return Fail(ErrorCode::REPLICA_IS_GONE,
                            "missing segment instance identity");
            }
            tasks.push_back({record_index, slice_index, &slice});
        }
    }
    std::sort(tasks.begin(), tasks.end(), [](const auto& lhs, const auto& rhs) {
        return std::tie(lhs.slice->target_segment_name,
                        lhs.slice->target_offset) <
               std::tie(rhs.slice->target_segment_name,
                        rhs.slice->target_offset);
    });
    for (size_t i = 1; i < tasks.size(); ++i) {
        const auto& previous = *tasks[i - 1].slice;
        const auto& current = *tasks[i].slice;
        if (previous.target_segment_name == current.target_segment_name &&
            previous.target_offset <=
                std::numeric_limits<uint64_t>::max() -
                    previous.allocated_length &&
            previous.target_offset + previous.allocated_length >
                current.target_offset) {
            return Fail(ErrorCode::INVALID_PARAMS,
                        "overlapping recovery claims");
        }
    }

    VChunkRecoveryView view;
    view.leader_epoch = leader_epoch;
    view.entries.resize(records.size());
    for (size_t i = 0; i < records.size(); ++i) {
        view.entries[i].record = std::move(records[i]);
        view.entries[i].claims.resize(view.entries[i].record.slices.size());
    }

    phase_ = VChunkRecoveryPhase::RESOURCE_CLAIMING;
    for (const auto& task : tasks) {
        auto& entry = view.entries[task.record_index];
        const auto& slice = entry.record.slices[task.slice_index];
        auto allocator = FindAllocator(allocators, slice);
        if (!allocator || !(*allocator)->supportsExactClaim()) {
            return Fail(allocator ? ErrorCode::UNAVAILABLE_IN_CURRENT_MODE
                                  : allocator.error(),
                        "allocator cannot claim historical address");
        }
        AllocationClaim claim;
        claim.segment_name = slice.target_segment_name;
        claim.segment_instance_id = slice.segment_instance_id;
        claim.offset = slice.target_offset;
        claim.allocated_length = slice.allocated_length;
        claim.owner_vchunk_id = entry.record.vchunk_id;
        claim.leader_epoch = leader_epoch;
        claim.generation = slice.allocation_generation;
        auto reserved = (*allocator)->reserveAt(claim);
        if (!reserved) {
            return Fail(reserved.error(), "address claim conflict");
        }
        entry.claims[task.slice_index] = std::move(*reserved);
    }

    phase_ = VChunkRecoveryPhase::DATA_VERIFYING;
    for (auto& entry : view.entries) {
        entry.record.leader_epoch = leader_epoch;
        ++entry.record.metadata_version;
        if (entry.record.status == VChunkStatus::ACTIVE) continue;
        if (entry.record.status == VChunkStatus::RELEASING ||
            entry.record.status == VChunkStatus::RELEASED ||
            entry.record.status == VChunkStatus::FAILED) {
            entry.record.status = VChunkStatus::ABANDONED;
        } else {
            entry.record.status = VChunkStatus::RECOVERING;
        }
    }
    phase_ = VChunkRecoveryPhase::INCOMPLETE_RECOVERING;
    return view;
}

}  // namespace mooncake
