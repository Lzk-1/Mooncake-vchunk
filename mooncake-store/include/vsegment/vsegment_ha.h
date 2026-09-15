#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "ha/oplog/ordered_oplog_writer.h"
#include "vsegment/vsegment_manager.h"

namespace mooncake::vsegment {

struct VSegmentOpLogRecord {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::string mutation;
    PartitionVSegmentSnapshot state;
};
YLT_REFL(VSegmentOpLogRecord, partition_id, route_epoch, metadata_revision,
         mutation, state);

class OrderedOpLogVSegmentCommitter final : public VSegmentStateCommitter {
   public:
    explicit OrderedOpLogVSegmentCommitter(
        OrderedOpLogWriter* writer,
        std::chrono::milliseconds durable_timeout =
            std::chrono::milliseconds(30000))
        : writer_(writer), durable_timeout_(durable_timeout) {}

    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* detail) override;

   private:
    OrderedOpLogWriter* writer_;
    std::chrono::milliseconds durable_timeout_;
};

// Replays only vsegment state records after a snapshot. Every record carries
// the resulting Partition vsegment state, making replay idempotent while the
// surrounding OpLog still provides ordering and durable-prefix semantics.
ErrorCode ReplayVSegmentState(
    const PartitionVSegmentSnapshot& base,
    const std::vector<OpLogEntry>& entries,
    PartitionVSegmentSnapshot* recovered,
    std::string* detail = nullptr);

}  // namespace mooncake::vsegment
