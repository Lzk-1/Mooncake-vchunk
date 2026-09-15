#include "vsegment/vsegment_ha.h"

#include <condition_variable>
#include <mutex>

namespace mooncake::vsegment {

ErrorCode OrderedOpLogVSegmentCommitter::Commit(
    const PartitionVSegmentSnapshot& state, const std::string& mutation,
    std::string* detail) {
    if (!writer_ || mutation.empty() || state.partition_id.empty() ||
        state.metadata_revision == 0) {
        if (detail) *detail = "invalid vsegment OpLog commit";
        return ErrorCode::INVALID_PARAMS;
    }
    VSegmentOpLogRecord record{state.partition_id, state.route_epoch,
                               state.metadata_revision, mutation, state};
    std::string payload;
    struct_json::to_json(record, payload);
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    entry.object_key = state.partition_id;
    entry.payload = std::move(payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);

    auto reservation = writer_->Reserve();
    if (!reservation) {
        if (detail) *detail = "OpLog writer is not accepting vsegment state";
        return reservation.error();
    }
    struct DurableWait {
        std::mutex mutex;
        std::condition_variable ready;
        bool durable{false};
    };
    auto wait = std::make_shared<DurableWait>();
    auto pending = writer_->Commit(
        std::move(*reservation), std::move(entry), [wait](const OpLogEntry&) {
            std::lock_guard<std::mutex> lock(wait->mutex);
            wait->durable = true;
            wait->ready.notify_one();
        });
    if (!pending) {
        if (detail) *detail = "failed to enqueue vsegment OpLog entry";
        return pending.error();
    }
    std::unique_lock<std::mutex> lock(wait->mutex);
    if (!wait->ready.wait_for(lock, durable_timeout_,
                              [&] { return wait->durable; })) {
        if (detail) *detail = "timed out waiting for durable vsegment OpLog";
        return ErrorCode::PERSISTENT_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode ReplayVSegmentState(
    const PartitionVSegmentSnapshot& base,
    const std::vector<OpLogEntry>& entries,
    PartitionVSegmentSnapshot* recovered, std::string* detail) {
    if (!recovered || base.partition_id.empty())
        return ErrorCode::INVALID_PARAMS;
    *recovered = base;
    uint64_t revision = base.metadata_revision;
    for (const auto& entry : entries) {
        if (entry.op_type != OpType::VSEGMENT_STATE) continue;
        if (!VerifyOpLogChecksum(entry)) {
            if (detail) *detail = "vsegment OpLog checksum mismatch";
            return ErrorCode::CHECKSUM_MISMATCH;
        }
        VSegmentOpLogRecord record;
        try {
            struct_json::from_json(record, entry.payload);
        } catch (const std::exception& error) {
            if (detail) *detail = error.what();
            return ErrorCode::INVALID_PARAMS;
        }
        if (record.partition_id != base.partition_id ||
            record.state.partition_id != base.partition_id) {
            if (detail) *detail = "vsegment OpLog Partition mismatch";
            return ErrorCode::INVALID_PARAMS;
        }
        if (record.metadata_revision <= revision) continue;
        if (record.metadata_revision != revision + 1) {
            if (detail) *detail = "vsegment OpLog revision gap";
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        if (record.route_epoch < recovered->route_epoch ||
            record.state.route_epoch != record.route_epoch) {
            if (detail) *detail = "stale vsegment route epoch";
            return ErrorCode::STALE_ROUTE;
        }
        if (record.state.metadata_revision != record.metadata_revision) {
            if (detail) *detail = "vsegment OpLog revision mismatch";
            return ErrorCode::INVALID_VERSION;
        }
        *recovered = std::move(record.state);
        revision = record.metadata_revision;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::vsegment
