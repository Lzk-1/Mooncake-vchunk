#include "vsegment/vsegment_transfer.h"

#include <limits>

namespace mooncake::vsegment {

namespace {

std::string ViewCacheKey(const std::string& partition_id,
                         const std::string& vsegment_id) {
    return partition_id + '\0' + vsegment_id;
}

bool SameImmutableView(const VSegmentView& left,
                       const VSegmentView& right) {
    if (left.vsegment_id != right.vsegment_id ||
        left.partition_id != right.partition_id ||
        left.profile_name != right.profile_name ||
        left.mapping_algorithm != right.mapping_algorithm ||
        left.stripe_size != right.stripe_size ||
        left.logical_capacity != right.logical_capacity ||
        left.members.size() != right.members.size()) {
        return false;
    }
    for (size_t index = 0; index < left.members.size(); ++index) {
        const auto& a = left.members[index];
        const auto& b = right.members[index];
        if (a.segment_id != b.segment_id || a.base_offset != b.base_offset ||
            a.length != b.length) {
            return false;
        }
    }
    return true;
}

}  // namespace

bool VSegmentViewCache::Find(const std::string& partition_id,
                             const std::string& vsegment_id,
                             VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = views_.find(ViewCacheKey(partition_id, vsegment_id));
    if (found == views_.end()) return false;
    if (view) *view = found->second;
    return true;
}

ErrorCode VSegmentViewCache::Insert(const VSegmentView& view,
                                    std::string* detail) {
    auto validation = ValidateViewStructure(view, detail);
    if (validation != ErrorCode::OK) return validation;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = ViewCacheKey(view.partition_id, view.vsegment_id);
    auto found = views_.find(key);
    if (found != views_.end()) {
        if (!SameImmutableView(found->second, view)) {
            if (detail) {
                *detail = "immutable view identity has another layout";
            }
            return ErrorCode::INVALID_VERSION;
        }
        return ErrorCode::OK;
    }
    views_.emplace(key, view);
    return ErrorCode::OK;
}

void VSegmentViewCache::Erase(const std::string& partition_id,
                              const std::string& vsegment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    views_.erase(ViewCacheKey(partition_id, vsegment_id));
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
    if (!cache_->Find(replica.partition_id, replica.vsegment_id, &view)) {
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
