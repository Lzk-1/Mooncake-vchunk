#include "vchunk_transfer_engine.h"

#include <algorithm>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace {

class BatchGuard {
   public:
    BatchGuard(TransferEngine& engine, size_t size)
        : engine_(engine), id_(engine.allocateBatchID(size)) {}
    ~BatchGuard() {
        TryFree();
    }
    BatchID id() const { return id_; }
    bool TryFree() {
        if (id_ == INVALID_BATCH_ID) return true;
        if (!engine_.freeBatchID(id_).ok()) return false;
        id_ = INVALID_BATCH_ID;
        return true;
    }

   private:
    TransferEngine& engine_;
    BatchID id_;
};

}  // namespace

tl::expected<std::vector<TransferRequest>, ErrorCode>
BuildVChunkTransferRequests(const VChunkMetadataRecord& record, void* buffer,
                            size_t length, TransferRequest::OpCode opcode,
                            const VChunkSegmentResolver& resolve_segment) {
    auto batches = BuildVChunkTransferBatches(record, buffer, length, opcode,
                                              resolve_segment, false);
    if (!batches) return tl::make_unexpected(batches.error());
    std::vector<TransferRequest> requests;
    for (auto& batch : *batches) {
        requests.insert(requests.end(), batch.requests.begin(),
                        batch.requests.end());
    }
    return requests;
}

tl::expected<std::vector<VChunkTransferBatch>, ErrorCode>
BuildVChunkTransferBatches(const VChunkMetadataRecord& record, void* buffer,
                           size_t length, TransferRequest::OpCode opcode,
                           const VChunkSegmentResolver& resolve_segment,
                           bool merge_adjacent_requests,
                           const std::unordered_set<std::string>&
                               excluded_segments) {
    VChunkConfig validation_config;
    validation_config.enabled = true;
    if (!buffer || !resolve_segment || record.total_size != length ||
        record.slices.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto validation = ValidateVChunkMetadata(record, validation_config);
    if (validation != ErrorCode::OK) {
        return tl::make_unexpected(validation);
    }
    std::unordered_map<std::string, SegmentHandle> handles;
    std::unordered_map<std::string, size_t> batch_positions;
    std::vector<VChunkTransferBatch> batches;
    auto resolve = [&](const VCSliceDescriptor& slice)
        -> tl::expected<SegmentHandle, ErrorCode> {
        auto it = handles.find(slice.target_segment_name);
        if (it == handles.end()) {
            auto handle = resolve_segment(slice.target_segment_name);
            if (!handle) {
                return tl::make_unexpected(handle.error());
            }
            it = handles.emplace(slice.target_segment_name, *handle).first;
        }
        return it->second;
    };
    auto append = [&](const VCSliceDescriptor& slice,
                      SegmentHandle handle) -> ErrorCode {
        const size_t logical_offset =
            static_cast<size_t>(slice.slice_index) *
            SliceSizeLevelToBytes(record.slice_size_level);
        if (logical_offset > length ||
            slice.logical_length > length - logical_offset) {
            return ErrorCode::INVALID_PARAMS;
        }
        auto [position, inserted] = batch_positions.emplace(
            slice.target_segment_name, batches.size());
        if (inserted) {
            batches.push_back(VChunkTransferBatch{slice.target_segment_name,
                                                  {}});
        }
        auto& requests = batches[position->second].requests;
        TransferRequest request{
            opcode, static_cast<char*>(buffer) + logical_offset, handle,
            slice.target_offset, slice.logical_length};
        if (merge_adjacent_requests && !requests.empty()) {
            auto& previous = requests.back();
            if (previous.target_id == request.target_id &&
                previous.target_offset + previous.length ==
                    request.target_offset &&
                static_cast<char*>(previous.source) + previous.length ==
                    request.source) {
                previous.length += request.length;
                return ErrorCode::OK;
            }
        }
        requests.push_back(request);
        return ErrorCode::OK;
    };

    if (opcode == TransferRequest::WRITE) {
        for (const auto& slice : record.slices) {
            auto handle = resolve(slice);
            if (!handle) return tl::make_unexpected(handle.error());
            const auto error = append(slice, *handle);
            if (error != ErrorCode::OK) {
                return tl::make_unexpected(error);
            }
        }
    } else {
        for (uint32_t slice_index = 0; slice_index < record.slice_count;
             ++slice_index) {
            ErrorCode last_error = ErrorCode::SEGMENT_NOT_FOUND;
            bool selected = false;
            for (uint8_t replica = 0; replica < record.replica_num; ++replica) {
                const auto& slice =
                    record.slices[static_cast<size_t>(replica) *
                                      record.slice_count +
                                  slice_index];
                if (slice.status == VCSliceStatus::FAILED) continue;
                if (excluded_segments.contains(slice.target_segment_name)) {
                    continue;
                }
                auto handle = resolve(slice);
                if (!handle) {
                    last_error = handle.error();
                    continue;
                }
                const auto error = append(slice, *handle);
                if (error != ErrorCode::OK) {
                    return tl::make_unexpected(error);
                }
                selected = true;
                break;
            }
            if (!selected) {
                return tl::make_unexpected(last_error);
            }
        }
    }
    if (batches.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (const auto& batch : batches) {
        if (batch.requests.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    return batches;
}

ErrorCode TransferEngineVChunkDataPlane::Write(
    const VChunkMetadataRecord& record, const void* source, size_t length,
    std::chrono::steady_clock::time_point deadline) {
    return Transfer(record, const_cast<void*>(source), length,
                    TransferRequest::WRITE, deadline, {}, nullptr);
}

ErrorCode TransferEngineVChunkDataPlane::Read(
    const VChunkMetadataRecord& record, void* destination, size_t length,
    std::chrono::steady_clock::time_point deadline) {
    return ReadAttempt(record, destination, length, deadline, {}).error;
}

VChunkDataPlane::ReadAttemptResult TransferEngineVChunkDataPlane::ReadAttempt(
    const VChunkMetadataRecord& record, void* destination, size_t length,
    std::chrono::steady_clock::time_point deadline,
    const std::unordered_set<std::string>& excluded_segments) {
    ReadAttemptResult result;
    result.error = Transfer(record, destination, length, TransferRequest::READ,
                            deadline, excluded_segments,
                            &result.failed_segments);
    return result;
}

ErrorCode TransferEngineVChunkDataPlane::Transfer(
    const VChunkMetadataRecord& record, void* buffer, size_t length,
    TransferRequest::OpCode opcode,
    std::chrono::steady_clock::time_point deadline,
    const std::unordered_set<std::string>& excluded_segments,
    std::vector<std::string>* failed_segments) {
    auto batches = BuildVChunkTransferBatches(
        record, buffer, length, opcode, [this](const std::string& segment) {
            const auto handle = engine_.openSegment(segment);
            if (handle == static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT)) {
                return tl::expected<SegmentHandle, ErrorCode>(
                    tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND));
            }
            return tl::expected<SegmentHandle, ErrorCode>(handle);
        }, true, excluded_segments);
    if (!batches) {
        return batches.error();
    }

    struct ActiveBatch {
        std::string segment_name;
        std::unique_ptr<BatchGuard> guard;
        size_t expected_bytes{0};
        bool finished{false};
        bool failed{false};
    };
    std::vector<ActiveBatch> active;
    active.reserve(batches->size());
    for (const auto& batch : *batches) {
        auto guard = std::make_unique<BatchGuard>(engine_, batch.requests.size());
        if (guard->id() == INVALID_BATCH_ID) {
            return ErrorCode::TRANSFER_FAIL;
        }
        size_t expected_bytes = 0;
        for (const auto& request : batch.requests) {
            expected_bytes += request.length;
        }
        active.push_back(ActiveBatch{batch.segment_name, std::move(guard),
                                     expected_bytes, false, false});
    }
    bool submit_failed = false;
    for (size_t i = 0; i < batches->size(); ++i) {
        bool finished = false;
        bool failed = false;
        auto& guard = active[i].guard;
        const auto& batch = (*batches)[i];
        if (!engine_.submitTransfer(guard->id(), batch.requests).ok()) {
            submit_failed = true;
            failed = true;
            finished = guard->TryFree();
        }
        active[i].finished = finished;
        active[i].failed = failed;
    }
    bool deadline_exceeded = false;
    for (;;) {
        deadline_exceeded = deadline_exceeded ||
                            std::chrono::steady_clock::now() >= deadline;
        bool all_finished = true;
        for (auto& batch : active) {
            if (batch.finished) continue;
            all_finished = false;
            TransferStatus status{};
            if (!engine_
                     .getBatchTransferStatus(batch.guard->id(), status)
                     .ok()) {
                continue;
            }
            if (status.s == TransferStatusEnum::COMPLETED) {
                batch.finished = true;
                batch.failed =
                    status.transferred_bytes != batch.expected_bytes;
            } else if (status.s == TransferStatusEnum::FAILED ||
                       status.s == TransferStatusEnum::TIMEOUT ||
                       status.s == TransferStatusEnum::CANCELED ||
                       status.s == TransferStatusEnum::INVALID) {
                batch.finished = true;
                batch.failed = true;
            }
        }
        all_finished = std::all_of(
            active.begin(), active.end(),
            [](const ActiveBatch& batch) { return batch.finished; });
        if (all_finished) {
            const bool transfer_failed =
                submit_failed ||
                std::any_of(active.begin(), active.end(),
                            [](const ActiveBatch& batch) {
                                return batch.failed;
                            });
            if (failed_segments != nullptr) {
                for (const auto& batch : active) {
                    if (batch.failed) {
                        failed_segments->push_back(batch.segment_name);
                    }
                }
            }
            if (deadline_exceeded) return ErrorCode::RPC_TIMEOUT;
            return transfer_failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

}  // namespace mooncake
