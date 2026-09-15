#include "vsegment/vsegment_ha.h"

#include <condition_variable>
#include <mutex>

#include "mooncake_logging.h"

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
    std::string validation_detail;
    if (!ValidateOpLogEntrySize(entry, &validation_detail)) {
        if (detail) *detail = std::move(validation_detail);
        return ErrorCode::INVALID_PARAMS;
    }

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
    // Once Commit accepts an entry it cannot be cancelled: the ordered writer
    // may persist it after any local timeout. Returning failure here would make
    // VSegmentManager roll back memory while recovery later replays the entry.
    // Preserve the write-ahead invariant by waiting for the durable callback;
    // the interval is only used to surface a prolonged backend outage.
    while (!wait->ready.wait_for(lock, durable_wait_warning_interval_,
                                 [&] { return wait->durable; })) {
        MC_LOG(WARNING)
            << "Still waiting for durable vsegment OpLog, sequence_id="
            << pending->sequence_id()
            << ", writer_error=" << writer_->LastError();
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
