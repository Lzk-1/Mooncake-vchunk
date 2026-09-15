#include "vsegment/vsegment_transfer.h"

#include <limits>

namespace mooncake::vsegment {

bool VSegmentViewCache::Find(const std::string& vsegment_id,
                             VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = views_.find(vsegment_id);
    if (found == views_.end()) return false;
    if (view) *view = found->second;
    return true;
}

ErrorCode VSegmentViewCache::Insert(const VSegmentView& view,
                                    std::string* detail) {
    auto validation = ValidateViewStructure(view, detail);
    if (validation != ErrorCode::OK) return validation;
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = views_.find(view.vsegment_id);
    if (found != views_.end()) {
        if (found->second.checksum != view.checksum) {
            if (detail) *detail = "immutable view identity has another checksum";
            return ErrorCode::INVALID_VERSION;
        }
        return ErrorCode::OK;
    }
    views_.emplace(view.vsegment_id, view);
    return ErrorCode::OK;
}

void VSegmentViewCache::Erase(const std::string& vsegment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    views_.erase(vsegment_id);
}

VSegmentTransferPlan VSegmentTransferPlanner::Plan(
    const VSegmentDescriptor& replica,
    const std::vector<ClientSlice>& slices) {
    VSegmentTransferPlan plan;
    if (!provider_ || !endpoint_resolver_ || !cache_ ||
        replica.partition_id.empty() || replica.vsegment_id.empty() ||
        replica.length == 0) {
        plan.error = ErrorCode::INVALID_PARAMS;
        plan.detail = "invalid vsegment transfer planner input";
        return plan;
    }
    VSegmentView view;
    if (!cache_->Find(replica.vsegment_id, &view)) {
        auto result = provider_->LoadView(replica.partition_id,
                                          replica.vsegment_id, &view,
                                          &plan.detail);
        if (result != ErrorCode::OK) {
            plan.error = result;
            return plan;
        }
        result = cache_->Insert(view, &plan.detail);
        if (result != ErrorCode::OK) {
            plan.error = result;
            return plan;
        }
    }
    if (view.partition_id != replica.partition_id) {
        plan.error = ErrorCode::INVALID_VERSION;
        plan.detail = "cached view belongs to another Partition";
        return plan;
    }
    auto resolved = ResolveTransfer(view, replica.logical_offset,
                                    replica.length, slices);
    if (!resolved) return {resolved.error, {}, std::move(resolved.detail)};
    for (auto& request : resolved.requests) {
        std::string endpoint;
        auto result = endpoint_resolver_->ResolveEndpoint(request.segment_id,
                                                          &endpoint);
        if (result != ErrorCode::OK || endpoint.empty()) {
            plan.error = result == ErrorCode::OK ? ErrorCode::SEGMENT_NOT_FOUND
                                                 : result;
            plan.detail = "cannot resolve endpoint for " + request.segment_id;
            plan.requests.clear();
            return plan;
        }
        plan.requests.push_back({std::move(request), std::move(endpoint)});
    }
    return plan;
}

}  // namespace mooncake::vsegment
