#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "vsegment/vsegment.h"

namespace mooncake::vsegment {

inline constexpr char kPartitionQuotaSnapshotKey[] =
    "/mooncake/vsegment/partition-quota/current";

struct PartitionQuotaPlanRequest {
    uint64_t config_generation{0};
    std::string policy_digest;
    std::string default_profile;
    std::vector<std::string> partition_ids;
    std::vector<VSegmentProfileSpec> profile_specs;
    std::vector<PSegmentGeometry> segments;
    // Fraction reserved outside vsegment quotas, in [0, 1).
    double reserved_ratio{0.0};
};
YLT_REFL(PartitionQuotaPlanRequest, config_generation, policy_digest,
         default_profile, partition_ids, profile_specs, segments,
         reserved_ratio);

struct PartitionQuotaPlanResult {
    ErrorCode error{ErrorCode::OK};
    PartitionPhysicalQuotaSnapshot snapshot;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};

class PartitionQuotaPlanner {
   public:
    PartitionQuotaPlanResult Plan(const PartitionQuotaPlanRequest& request) const;
};

class PartitionQuotaSnapshotStore {
   public:
    virtual ~PartitionQuotaSnapshotStore() = default;
    virtual ErrorCode Create(const PartitionPhysicalQuotaSnapshot& snapshot,
                             size_t max_serialized_bytes,
                             std::string* detail = nullptr) = 0;
    virtual ErrorCode Load(PartitionPhysicalQuotaSnapshot* snapshot,
                           std::string* detail = nullptr) = 0;
};

class EtcdPartitionQuotaSnapshotStore final
    : public PartitionQuotaSnapshotStore {
   public:
    explicit EtcdPartitionQuotaSnapshotStore(
        std::string key = kPartitionQuotaSnapshotKey)
        : key_(std::move(key)) {}

    ErrorCode Create(const PartitionPhysicalQuotaSnapshot& snapshot,
                     size_t max_serialized_bytes,
                     std::string* detail = nullptr) override;
    ErrorCode Load(PartitionPhysicalQuotaSnapshot* snapshot,
                   std::string* detail = nullptr) override;

   private:
    std::string key_;
};

}  // namespace mooncake::vsegment
