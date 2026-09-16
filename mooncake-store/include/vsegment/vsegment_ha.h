#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ha/oplog/ordered_oplog_writer.h"
#include "vsegment/vsegment_manager.h"

namespace mooncake::vsegment {

struct VSegmentStateDelta {
    std::vector<VSegmentStateSnapshot> upserted_vsegments;
    std::vector<std::string> removed_vsegment_ids;
    std::unordered_map<std::string, std::string> upserted_operations;
    std::vector<std::string> removed_operation_ids;
};
YLT_REFL(VSegmentStateDelta, upserted_vsegments, removed_vsegment_ids,
         upserted_operations, removed_operation_ids);

struct VSegmentOpLogRecord {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::string mutation;
    PartitionVSegmentSnapshot state;
    // Legacy records contain only `state` and therefore deserialize with the
    // default true. New records use one full checkpoint per committer lifetime
    // followed by compact deltas.
    bool full_state{true};
    VSegmentStateDelta delta;
};
YLT_REFL(VSegmentOpLogRecord, partition_id, route_epoch, metadata_revision,
         mutation, state, full_state, delta);

class OrderedOpLogVSegmentCommitter final : public VSegmentStateCommitter {
   public:
    explicit OrderedOpLogVSegmentCommitter(
        OrderedOpLogWriter* writer,
        std::chrono::milliseconds durable_wait_warning_interval =
            std::chrono::milliseconds(30000))
        : writer_(writer),
          durable_wait_warning_interval_(
              durable_wait_warning_interval.count() > 0
                  ? durable_wait_warning_interval
                  : std::chrono::milliseconds(30000)) {}

    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* detail) override;

   private:
    OrderedOpLogWriter* writer_;
    std::chrono::milliseconds durable_wait_warning_interval_;
    std::mutex mutex_;
    std::unordered_map<std::string, PartitionVSegmentSnapshot> last_states_;
};

// Replays legacy full-state and current delta records after a snapshot.
ErrorCode ReplayVSegmentState(
    const PartitionVSegmentSnapshot& base,
    const std::vector<OpLogEntry>& entries,
    PartitionVSegmentSnapshot* recovered,
    std::string* detail = nullptr);

}  // namespace mooncake::vsegment
