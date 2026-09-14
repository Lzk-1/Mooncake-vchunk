#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment.h"

namespace mooncake::vsegment {

struct LogicalRange {
    uint64_t offset{0};
    uint64_t length{0};
};
YLT_REFL(LogicalRange, offset, length);

struct ReservationResult {
    ErrorCode error{ErrorCode::OK};
    LogicalRange range;
    std::string detail;

    explicit operator bool() const { return error == ErrorCode::OK; }
};

struct ReservationRecord {
    std::string operation_id;
    LogicalRange range;
};
YLT_REFL(ReservationRecord, operation_id, range);

struct LogicalAllocationSnapshot {
    uint64_t logical_capacity{0};
    std::vector<LogicalRange> free_ranges;
    std::vector<ReservationRecord> reservations;
};
YLT_REFL(LogicalAllocationSnapshot, logical_capacity, free_ranges,
         reservations);

// Tracks only free space and in-flight PutStart reservations. Committed ranges
// are represented by ObjectMetadata and are returned through Release().
class LogicalRangeAllocator {
   public:
    explicit LogicalRangeAllocator(uint64_t logical_capacity);

    ReservationResult Reserve(const std::string& operation_id,
                              uint64_t length);
    ErrorCode Commit(const std::string& operation_id, LogicalRange* range);
    ErrorCode Abort(const std::string& operation_id);
    ErrorCode Release(LogicalRange range);

    LogicalAllocationSnapshot Snapshot() const;
    ErrorCode Restore(const LogicalAllocationSnapshot& snapshot,
                      std::string* detail = nullptr);

    uint64_t FreeBytes() const;
    size_t ReservationCount() const;

   private:
    static void InsertAndMerge(std::vector<LogicalRange>& ranges,
                               LogicalRange range);
    bool OverlapsFreeOrReserved(LogicalRange range) const;

    uint64_t logical_capacity_;
    mutable std::mutex mutex_;
    std::vector<LogicalRange> free_ranges_;
    std::unordered_map<std::string, LogicalRange> reservations_;
};

struct ClientSlice {
    uint64_t address{0};
    uint64_t length{0};
};

struct TransferSubRequest {
    uint64_t sequence{0};
    uint64_t client_buffer_offset{0};
    uint64_t client_address{0};
    std::string segment_id;
    uint64_t physical_offset{0};
    uint64_t length{0};
};

struct ResolveResult {
    ErrorCode error{ErrorCode::OK};
    std::vector<TransferSubRequest> requests;
    std::string detail;

    explicit operator bool() const { return error == ErrorCode::OK; }
};

// Splits on both stripe and Client Slice boundaries. The output preserves
// logical order, while execution may complete in any order.
ResolveResult ResolveTransfer(const VSegmentView& view,
                              uint64_t logical_offset, uint64_t length,
                              const std::vector<ClientSlice>& slices);

// Serializes creation for the same creation_key. Different keys may execute
// concurrently, and successful results remain cached for subsequent callers.
class CreationCoordinator {
   public:
    using Factory = std::function<VSegmentAllocationResult()>;

    VSegmentAllocationResult GetOrCreate(const std::string& creation_key,
                                         Factory factory);
    void Forget(const std::string& creation_key);

   private:
    struct Entry {
        bool creating{true};
        VSegmentAllocationResult result;
        std::condition_variable ready;
    };

    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
};

}  // namespace mooncake::vsegment
