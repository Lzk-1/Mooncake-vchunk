#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "replica.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {

class VSegmentViewProvider {
   public:
    virtual ~VSegmentViewProvider() = default;
    virtual ErrorCode LoadView(const std::string& partition_id,
                               const std::string& vsegment_id,
                               VSegmentView* view,
                               std::string* detail = nullptr) = 0;
};

class SegmentEndpointResolver {
   public:
    virtual ~SegmentEndpointResolver() = default;
    virtual ErrorCode ResolveEndpoint(const std::string& segment_id,
                                      std::string* endpoint) = 0;
};

class VSegmentViewCache {
   public:
    bool Find(const std::string& vsegment_id, VSegmentView* view) const;
    ErrorCode Insert(const VSegmentView& view,
                     std::string* detail = nullptr);
    void Erase(const std::string& vsegment_id);

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, VSegmentView> views_;
};

struct EndpointTransferSubRequest {
    TransferSubRequest transfer;
    std::string endpoint;
};

struct VSegmentTransferPlan {
    ErrorCode error{ErrorCode::OK};
    std::vector<EndpointTransferSubRequest> requests;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};

class VSegmentTransferPlanner {
   public:
    VSegmentTransferPlanner(VSegmentViewProvider* provider,
                            SegmentEndpointResolver* endpoint_resolver,
                            VSegmentViewCache* cache)
        : provider_(provider),
          endpoint_resolver_(endpoint_resolver),
          cache_(cache) {}

    VSegmentTransferPlan Plan(const VSegmentDescriptor& replica,
                              const std::vector<ClientSlice>& slices);

   private:
    VSegmentViewProvider* provider_;
    SegmentEndpointResolver* endpoint_resolver_;
    VSegmentViewCache* cache_;
};

}  // namespace mooncake::vsegment
