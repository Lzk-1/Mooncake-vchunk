#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {
namespace cvm {

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

// Ownership state of a logical KV slot.
enum class SlotState : int32_t {
    kStable = 0,     // Served by primary_master_id.
    kMigrating = 1,  // Handing off to migrating_to_master_id.
};

// Role of a master within the CVM topology.
enum class MasterRole : int32_t {
    kPrimary = 0,
    kStandby = 1,
};

// ---------------------------------------------------------------------------
// KV slot ownership record
// ---------------------------------------------------------------------------

// NOTE: enum fields are stored as int32_t so the records serialize/deserialize
// identically across languages and compiler settings.
struct SlotOwner {
    uint16_t slot{0};
    std::string primary_master_id;
    int32_t state{0};  // SlotState
    std::string migrating_to_master_id;  // empty when stable
};
YLT_REFL(SlotOwner, slot, primary_master_id, state, migrating_to_master_id);

// ---------------------------------------------------------------------------
// Segment neutral entity (one per segment, no owner) — replaces the old
// single-owner SegmentView model.  Stored under segments/{segment_id}.
// ---------------------------------------------------------------------------

// Lightweight per-master mount record.  key = (master_id, segment_id) naturally
// supports one-segment-multi-master mounting without overwrite conflicts.
struct MountEntry {
    std::string segment_id;
    int64_t mounted_at_ms{0};
    // Future: partition_slot_starts for psegment/vsegment partial mount
    std::vector<uint16_t> partition_slot_starts;
};
YLT_REFL(MountEntry, segment_id, mounted_at_ms, partition_slot_starts);

// Neutral descriptor of a segment — authoritative copy lives under
// segments/{segment_id} and is written once per segment (idempotent).
struct SegmentDescriptor {
    std::string segment_id;
    std::string segment_name;
    size_t capacity{0};
    std::string te_endpoint;  // transport endpoint, e.g. "host:port"
    std::string protocol;
    std::string host_id;
    // Future: partitions for psegment/vsegment split
    struct Partition {
        uint16_t slot_start{0};
        uint16_t slot_end{0};
        uint64_t offset{0};
        uint64_t length{0};
    };
    std::vector<Partition> partitions;
};
YLT_REFL(SegmentDescriptor, segment_id, segment_name, capacity, te_endpoint,
         protocol, host_id, partitions);

// ---------------------------------------------------------------------------
// Master registration: liveness + role, persisted under an etcd lease
// ---------------------------------------------------------------------------

struct MasterRegistration {
    std::string master_id;
    std::string address;  // RPC endpoint, e.g. "host:port"
    int32_t role{0};      // MasterRole
    int64_t registered_at_ms{0};
};
YLT_REFL(MasterRegistration, master_id, address, role, registered_at_ms);

// Cluster-wide ring configuration persisted under /cvm/{ns}/cluster_meta
// (确定性哈希方案 §15.3). Clients and masters derive the same primary_ids =
// sort(members by master_id)[0:submaster_count] from this count, so slot
// ownership no longer needs to be persisted per slot.
struct RingMeta {
    uint32_t submaster_count{1};
};
YLT_REFL(RingMeta, submaster_count);

}  // namespace cvm
}  // namespace mooncake