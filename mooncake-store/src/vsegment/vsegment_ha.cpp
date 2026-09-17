#include "vsegment/vsegment_ha.h"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <unordered_set>

#include "mooncake_logging.h"

namespace mooncake::vsegment {
namespace {

bool SameState(const VSegmentStateSnapshot& left,
               const VSegmentStateSnapshot& right) {
    std::string left_json;
    std::string right_json;
    struct_json::to_json(left, left_json);
    struct_json::to_json(right, right_json);
    return left_json == right_json;
}

VSegmentStateDelta BuildDelta(const PartitionVSegmentSnapshot& before,
                              const PartitionVSegmentSnapshot& after) {
    VSegmentStateDelta delta;
    std::unordered_map<std::string, const VSegmentStateSnapshot*> old;
    for (const auto& item : before.vsegments)
        old.emplace(item.view.vsegment_id, &item);
    std::unordered_set<std::string> present;
    for (const auto& item : after.vsegments) {
        present.insert(item.view.vsegment_id);
        auto found = old.find(item.view.vsegment_id);
        if (found == old.end() || !SameState(*found->second, item))
            delta.upserted_vsegments.push_back(item);
    }
    for (const auto& item : before.vsegments) {
        if (!present.count(item.view.vsegment_id))
            delta.removed_vsegment_ids.push_back(item.view.vsegment_id);
    }
    for (const auto& [operation, vsegment] : after.operation_vsegments) {
        auto found = before.operation_vsegments.find(operation);
        if (found == before.operation_vsegments.end() ||
            found->second != vsegment)
            delta.upserted_operations.emplace(operation, vsegment);
    }
    for (const auto& [operation, vsegment] : before.operation_vsegments) {
        (void)vsegment;
        if (!after.operation_vsegments.count(operation))
            delta.removed_operation_ids.push_back(operation);
    }
    return delta;
}

ErrorCode ApplyDelta(const VSegmentOpLogRecord& record,
                     PartitionVSegmentSnapshot* state,
                     std::string* detail) {
    std::unordered_set<std::string> removed(
        record.delta.removed_vsegment_ids.begin(),
        record.delta.removed_vsegment_ids.end());
    state->vsegments.erase(
        std::remove_if(state->vsegments.begin(), state->vsegments.end(),
                       [&](const auto& item) {
                           return removed.count(item.view.vsegment_id) != 0;
                       }),
        state->vsegments.end());
    for (const auto& replacement : record.delta.upserted_vsegments) {
        if (replacement.view.vsegment_id.empty()) {
            if (detail) *detail = "delta contains an empty vsegment id";
            return ErrorCode::INVALID_PARAMS;
        }
        auto found = std::find_if(
            state->vsegments.begin(), state->vsegments.end(),
            [&](const auto& item) {
                return item.view.vsegment_id == replacement.view.vsegment_id;
            });
        if (found == state->vsegments.end())
            state->vsegments.push_back(replacement);
        else
            *found = replacement;
    }
    for (const auto& operation : record.delta.removed_operation_ids)
        state->operation_vsegments.erase(operation);
    for (const auto& [operation, vsegment] :
         record.delta.upserted_operations)
        state->operation_vsegments[operation] = vsegment;
    state->route_epoch = record.route_epoch;
    state->metadata_revision = record.metadata_revision;
    return ErrorCode::OK;
}

}  // namespace

ErrorCode OrderedOpLogVSegmentCommitter::Commit(
    const PartitionVSegmentSnapshot& state, const std::string& mutation,
    std::string* detail) {
    if (!writer_ || mutation.empty() || state.partition_id.empty() ||
        state.metadata_revision == 0) {
        if (detail) *detail = "invalid vsegment OpLog commit";
        return ErrorCode::INVALID_PARAMS;
    }
    std::unique_lock<std::mutex> commit_lock(mutex_);
    VSegmentOpLogRecord record;
    record.partition_id = state.partition_id;
    record.route_epoch = state.route_epoch;
    record.metadata_revision = state.metadata_revision;
    record.mutation = mutation;
    auto previous = last_states_.find(state.partition_id);
    if (previous == last_states_.end() ||
        previous->second.metadata_revision + 1 != state.metadata_revision) {
        record.full_state = true;
        record.state = state;
    } else {
        record.full_state = false;
        record.delta = BuildDelta(previous->second, state);
    }
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
    // REMOVE/PUT_END durable callbacks may release vsegment allocations. A
    // synchronous wait from the writer's callback thread would wait for a
    // callback queued behind itself. The object-metadata record is already
    // durable in this context and is authoritative during recovery, so the
    // accepted vsegment delta may complete asynchronously without weakening
    // crash consistency.
    if (writer_->IsCallbackThread()) {
        last_states_[state.partition_id] = state;
        return ErrorCode::OK;
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
    last_states_[state.partition_id] = state;
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
            (record.full_state &&
             record.state.partition_id != base.partition_id)) {
            if (detail) *detail = "vsegment OpLog Partition mismatch";
            return ErrorCode::INVALID_PARAMS;
        }
        if (record.metadata_revision <= revision) continue;
        if (record.route_epoch < recovered->route_epoch ||
            (record.full_state &&
             record.state.route_epoch != record.route_epoch)) {
            if (detail) *detail = "stale vsegment route epoch";
            return ErrorCode::STALE_ROUTE;
        }
        if (record.metadata_revision != revision + 1) {
            if (detail) *detail = "vsegment OpLog revision gap";
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        if (record.full_state &&
            record.state.metadata_revision != record.metadata_revision) {
            if (detail) *detail = "vsegment OpLog revision mismatch";
            return ErrorCode::INVALID_VERSION;
        }
        if (record.full_state) {
            *recovered = std::move(record.state);
        } else {
            const auto applied = ApplyDelta(record, recovered, detail);
            if (applied != ErrorCode::OK) return applied;
        }
        revision = record.metadata_revision;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::vsegment
