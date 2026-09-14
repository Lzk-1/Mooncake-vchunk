#include "vsegment/vsegment_runtime.h"

#include <algorithm>
#include <exception>
#include <limits>

namespace mooncake::vsegment {
namespace {

bool AddOverflows(uint64_t left, uint64_t right) {
    return right > std::numeric_limits<uint64_t>::max() - left;
}

bool Overlaps(LogicalRange left, LogicalRange right) {
    return left.offset < right.offset + right.length &&
           right.offset < left.offset + left.length;
}

ReservationResult ReservationError(ErrorCode error, std::string detail) {
    ReservationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

ResolveResult ResolveError(ErrorCode error, std::string detail) {
    ResolveResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

}  // namespace

LogicalRangeAllocator::LogicalRangeAllocator(uint64_t logical_capacity)
    : logical_capacity_(logical_capacity) {
    if (logical_capacity > 0) free_ranges_.push_back({0, logical_capacity});
}

ReservationResult LogicalRangeAllocator::Reserve(
    const std::string& operation_id, uint64_t length) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (operation_id.empty() || length == 0) {
        return ReservationError(ErrorCode::INVALID_PARAMS,
                                "operation_id and length are required");
    }
    auto existing = reservations_.find(operation_id);
    if (existing != reservations_.end()) {
        if (existing->second.length != length) {
            return ReservationError(
                ErrorCode::INVALID_PARAMS,
                "operation_id was already reserved with another length");
        }
        return {ErrorCode::OK, existing->second, {}};
    }

    auto range = std::find_if(free_ranges_.begin(), free_ranges_.end(),
                              [&](const auto& candidate) {
                                  return candidate.length >= length;
                              });
    if (range == free_ranges_.end()) {
        return ReservationError(ErrorCode::NO_AVAILABLE_HANDLE,
                                "vsegment logical space is insufficient");
    }
    LogicalRange reserved{range->offset, length};
    range->offset += length;
    range->length -= length;
    if (range->length == 0) free_ranges_.erase(range);
    reservations_.emplace(operation_id, reserved);
    return {ErrorCode::OK, reserved, {}};
}

ErrorCode LogicalRangeAllocator::Commit(const std::string& operation_id,
                                        LogicalRange* range) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto reservation = reservations_.find(operation_id);
    if (reservation == reservations_.end()) return ErrorCode::INVALID_WRITE;
    if (range) *range = reservation->second;
    reservations_.erase(reservation);
    return ErrorCode::OK;
}

void LogicalRangeAllocator::InsertAndMerge(
    std::vector<LogicalRange>& ranges, LogicalRange range) {
    ranges.push_back(range);
    std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
        return a.offset < b.offset;
    });
    std::vector<LogicalRange> merged;
    for (const auto& current : ranges) {
        if (!merged.empty() &&
            merged.back().offset + merged.back().length == current.offset) {
            merged.back().length += current.length;
        } else {
            merged.push_back(current);
        }
    }
    ranges.swap(merged);
}

ErrorCode LogicalRangeAllocator::Abort(const std::string& operation_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto reservation = reservations_.find(operation_id);
    if (reservation == reservations_.end()) return ErrorCode::INVALID_WRITE;
    InsertAndMerge(free_ranges_, reservation->second);
    reservations_.erase(reservation);
    return ErrorCode::OK;
}

bool LogicalRangeAllocator::OverlapsFreeOrReserved(LogicalRange range) const {
    for (const auto& free : free_ranges_) {
        if (Overlaps(range, free)) return true;
    }
    for (const auto& [operation, reserved] : reservations_) {
        if (Overlaps(range, reserved)) return true;
    }
    return false;
}

ErrorCode LogicalRangeAllocator::Release(LogicalRange range) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (range.length == 0 || AddOverflows(range.offset, range.length) ||
        range.offset + range.length > logical_capacity_ ||
        OverlapsFreeOrReserved(range)) {
        return ErrorCode::INVALID_PARAMS;
    }
    InsertAndMerge(free_ranges_, range);
    return ErrorCode::OK;
}

LogicalAllocationSnapshot LogicalRangeAllocator::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    LogicalAllocationSnapshot snapshot;
    snapshot.logical_capacity = logical_capacity_;
    snapshot.free_ranges = free_ranges_;
    for (const auto& [operation_id, range] : reservations_) {
        snapshot.reservations.push_back({operation_id, range});
    }
    std::sort(snapshot.reservations.begin(), snapshot.reservations.end(),
              [](const auto& left, const auto& right) {
                  return left.operation_id < right.operation_id;
              });
    return snapshot;
}

ErrorCode LogicalRangeAllocator::Restore(
    const LogicalAllocationSnapshot& snapshot, std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (snapshot.logical_capacity != logical_capacity_) {
        if (detail) *detail = "logical capacity changed during restore";
        return ErrorCode::INVALID_PARAMS;
    }
    std::vector<LogicalRange> all_ranges = snapshot.free_ranges;
    std::unordered_map<std::string, LogicalRange> reservations;
    for (const auto& reservation : snapshot.reservations) {
        if (reservation.operation_id.empty() ||
            !reservations.emplace(reservation.operation_id, reservation.range)
                 .second) {
            if (detail) *detail = "duplicate or empty reservation identity";
            return ErrorCode::INVALID_PARAMS;
        }
        all_ranges.push_back(reservation.range);
    }
    std::sort(all_ranges.begin(), all_ranges.end(),
              [](const auto& left, const auto& right) {
                  return left.offset < right.offset;
              });
    for (size_t index = 0; index < all_ranges.size(); ++index) {
        const auto& range = all_ranges[index];
        if (range.length == 0 || AddOverflows(range.offset, range.length) ||
            range.offset + range.length > logical_capacity_ ||
            (index > 0 && all_ranges[index - 1].offset +
                                      all_ranges[index - 1].length >
                                  range.offset)) {
            if (detail) *detail = "invalid or overlapping logical ranges";
            return ErrorCode::INVALID_PARAMS;
        }
    }
    free_ranges_ = snapshot.free_ranges;
    std::sort(free_ranges_.begin(), free_ranges_.end(),
              [](const auto& left, const auto& right) {
                  return left.offset < right.offset;
              });
    reservations_ = std::move(reservations);
    return ErrorCode::OK;
}

uint64_t LogicalRangeAllocator::FreeBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t bytes = 0;
    for (const auto& range : free_ranges_) bytes += range.length;
    return bytes;
}

size_t LogicalRangeAllocator::ReservationCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return reservations_.size();
}

ResolveResult ResolveTransfer(const VSegmentView& view,
                              uint64_t logical_offset, uint64_t length,
                              const std::vector<ClientSlice>& slices) {
    if (view.stripe_size == 0 || view.members.empty() || length == 0 ||
        AddOverflows(logical_offset, length) ||
        logical_offset + length > view.logical_capacity) {
        return ResolveError(ErrorCode::INVALID_PARAMS,
                            "invalid view or logical range");
    }
    uint64_t buffer_length = 0;
    for (const auto& slice : slices) {
        if (slice.length == 0 || AddOverflows(slice.address, slice.length) ||
            AddOverflows(buffer_length, slice.length)) {
            return ResolveError(ErrorCode::INVALID_PARAMS,
                                "invalid Client Slice");
        }
        buffer_length += slice.length;
    }
    if (buffer_length != length) {
        return ResolveError(ErrorCode::INVALID_PARAMS,
                            "Client Slice length does not match request");
    }

    ResolveResult result;
    uint64_t remaining = length;
    uint64_t current_logical = logical_offset;
    uint64_t client_offset = 0;
    size_t slice_index = 0;
    uint64_t offset_in_slice = 0;
    while (remaining > 0) {
        const uint64_t stripe_index = current_logical / view.stripe_size;
        const uint64_t offset_in_stripe = current_logical % view.stripe_size;
        const size_t member_index = stripe_index % view.members.size();
        const uint64_t member_round = stripe_index / view.members.size();
        const auto& member = view.members[member_index];
        if (AddOverflows(member.base_offset, offset_in_stripe) ||
            member_round >
            (std::numeric_limits<uint64_t>::max() - member.base_offset -
             offset_in_stripe) /
                view.stripe_size) {
            return ResolveError(ErrorCode::INVALID_PARAMS,
                                "physical offset overflows");
        }
        const uint64_t physical_offset =
            member.base_offset + member_round * view.stripe_size +
            offset_in_stripe;
        const uint64_t stripe_remaining =
            view.stripe_size - offset_in_stripe;
        const uint64_t slice_remaining =
            slices[slice_index].length - offset_in_slice;
        const uint64_t sub_length =
            std::min({remaining, stripe_remaining, slice_remaining});
        if (AddOverflows(member.base_offset, member.length) ||
            physical_offset < member.base_offset ||
            physical_offset > member.base_offset + member.length ||
            sub_length > member.base_offset + member.length - physical_offset) {
            return ResolveError(ErrorCode::INVALID_PARAMS,
                                "mapping exceeds member extent");
        }
        result.requests.push_back(
            {static_cast<uint32_t>(result.requests.size()), client_offset,
             slices[slice_index].address + offset_in_slice,
             member.segment_id, physical_offset, sub_length});

        remaining -= sub_length;
        current_logical += sub_length;
        client_offset += sub_length;
        offset_in_slice += sub_length;
        if (offset_in_slice == slices[slice_index].length) {
            ++slice_index;
            offset_in_slice = 0;
        }
    }
    return result;
}

VSegmentAllocationResult CreationCoordinator::GetOrCreate(
    const std::string& creation_key, Factory factory) {
    if (creation_key.empty() || !factory) {
        return {ErrorCode::INVALID_PARAMS, {},
                "creation_key and factory are required"};
    }
    std::shared_ptr<Entry> entry;
    bool creator = false;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto [iterator, inserted] =
            entries_.emplace(creation_key, std::make_shared<Entry>());
        entry = iterator->second;
        creator = inserted;
        if (!creator) {
            entry->ready.wait(lock, [&] { return !entry->creating; });
            return entry->result;
        }
    }

    VSegmentAllocationResult result;
    try {
        result = factory();
    } catch (const std::exception& exception) {
        result.error = ErrorCode::INTERNAL_ERROR;
        result.detail = exception.what();
    } catch (...) {
        result.error = ErrorCode::INTERNAL_ERROR;
        result.detail = "vsegment creation failed with an unknown exception";
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        entry->result = result;
        entry->creating = false;
    }
    entry->ready.notify_all();
    return result;
}

void CreationCoordinator::Forget(const std::string& creation_key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = entries_.find(creation_key);
    if (entry != entries_.end() && !entry->second->creating) {
        entries_.erase(entry);
    }
}

}  // namespace mooncake::vsegment
