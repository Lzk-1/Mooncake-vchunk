#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cvm/cvm_types.h"
#include "cvm/slot_migrator.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Drives slot-metadata handoff on a fixed interval (确定性哈希方案 §15.6).
//
// Slot ownership is NO LONGER published to etcd: it is derived
// deterministically from the primary member list (f(members, master_id)).
// On each tick this recomputes the owned slot set via `dynamic_slot_resolver`
// (or the static `owned_slots`) and hands the diff to SlotMigrator, which
// invokes the acquire/release hooks:
//   on_slot_acquired(slot) -> ErrorCode (RPC pull from the previous owner;
//                             non-OK keeps the slot pending for a retry)
//   on_slot_released(slot)  -> stage the export for the new owner to pull.
// No SlotOwner record is written anywhere; clients derive the same ring
// locally instead of reading a persisted kv_view.
class SlotOwnerHeartbeat {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        std::chrono::milliseconds heartbeat_interval{5000};

        // Slots owned by this submaster. When empty, the submaster owns every
        // logical slot (single-master mode).
        std::vector<uint16_t> owned_slots;

        // Optional resolver that recomputes the owned slot set on every
        // publish. When set, it overrides `owned_slots` and is invoked before
        // each PublishOnce so ownership tracks cluster membership changes
        // (dynamic partition). An empty result means the submaster owns no
        // slot. The callback must be safe to invoke from the heartbeat thread.
        std::function<std::vector<uint16_t>()> dynamic_slot_resolver;

        // Retained for diagnostics only: the supervisor-granted member lease
        // (member-level fence). No per-slot record is written with this lease
        // anymore — slot ownership is derived, not published.
        EtcdLeaseId lease_id{0};

        // Optional hooks fired on slot ownership change (确定性哈希方案 §15.7).
        // on_slot_acquired pulls object metadata for a newly-owned slot from the
        // previous owner and returns ErrorCode: OK marks it ready, any other
        // code keeps the slot pending for a retry next cycle. on_slot_released
        // stages the export for the new owner to pull. Leave empty for
        // derive-only behavior.
        std::function<ErrorCode(uint16_t)> on_slot_acquired;
        std::function<void(uint16_t)> on_slot_released;
    };

    explicit SlotOwnerHeartbeat(Config config);
    ~SlotOwnerHeartbeat();

    SlotOwnerHeartbeat(const SlotOwnerHeartbeat&) = delete;
    SlotOwnerHeartbeat& operator=(const SlotOwnerHeartbeat&) = delete;

    ErrorCode Start();
    void Stop();

    // Recomputes the owned slot set and drives the SlotMigrator diff/hooks
    // once. Idempotent; safe to call from the heartbeat thread or externally.
    ErrorCode PublishOnce();

   private:
    void RunLoop();

    Config config_;
    std::vector<uint16_t> owned_slots_;
    // Owns the slot handoff state machine (kMigrating -> kStable / release).
    SlotMigrator migrator_;

    std::atomic<bool> running_{false};
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
    std::thread thread_;
};

}  // namespace cvm
}  // namespace mooncake
