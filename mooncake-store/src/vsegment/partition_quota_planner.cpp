#include "vsegment/partition_quota_planner.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

#include "etcd_helper.h"

namespace mooncake::vsegment {
namespace {

PartitionQuotaPlanResult Fail(ErrorCode error, std::string detail) {
    return {error, {}, std::move(detail)};
}

uint64_t AlignDown(uint64_t value, uint64_t alignment) {
    return value - value % alignment;
}

}  // namespace

PartitionQuotaPlanResult PartitionQuotaPlanner::Plan(
    const PartitionQuotaPlanRequest& request) const {
    if (request.config_generation == 0 || request.policy_digest.empty() ||
        request.default_profile.empty() || request.partition_ids.empty() ||
        request.profile_specs.empty() || request.segments.empty() ||
        request.reserved_ratio < 0.0 || request.reserved_ratio >= 1.0) {
        return Fail(ErrorCode::INVALID_PARAMS,
                    "invalid quota planning request");
    }
    std::set<std::string> partition_ids;
    for (const auto& id : request.partition_ids) {
        if (id.empty() || !partition_ids.insert(id).second)
            return Fail(ErrorCode::INVALID_PARAMS,
                        "empty or duplicate partition id");
    }
    std::vector<std::string> ordered_partitions(partition_ids.begin(),
                                                 partition_ids.end());

    PartitionPhysicalQuotaSnapshot snapshot;
    snapshot.config_generation = request.config_generation;
    snapshot.policy_digest = request.policy_digest;
    snapshot.default_profile = request.default_profile;
    snapshot.profile_specs = request.profile_specs;
    std::sort(snapshot.profile_specs.begin(), snapshot.profile_specs.end(),
              [](const auto& a, const auto& b) { return a.name < b.name; });
    if (std::adjacent_find(snapshot.profile_specs.begin(),
                           snapshot.profile_specs.end(),
                           [](const auto& a, const auto& b) {
                               return a.name == b.name;
                           }) != snapshot.profile_specs.end())
        return Fail(ErrorCode::INVALID_PARAMS, "duplicate profile name");

    std::map<std::string, std::vector<PSegmentGeometry>> by_medium;
    for (const auto& segment : request.segments) {
        if (segment.healthy && segment.supports_unaligned_io)
            by_medium[segment.medium].push_back(segment);
    }
    for (auto& [medium, segments] : by_medium)
        std::sort(segments.begin(), segments.end(), [](const auto& a,
                                                       const auto& b) {
            return a.segment_id < b.segment_id;
        });

    // A physical byte can belong to only one profile. Profiles sharing a
    // medium receive deterministic contiguous lanes before each lane is split
    // evenly across Partitions.
    std::map<std::string, std::vector<const VSegmentProfileSpec*>> profiles;
    for (const auto& profile : snapshot.profile_specs) {
        std::string detail;
        if (ValidateProfile(profile, &detail) != ErrorCode::OK)
            return Fail(ErrorCode::INVALID_PARAMS, std::move(detail));
        profiles[profile.required_medium].push_back(&profile);
    }
    if (std::none_of(snapshot.profile_specs.begin(), snapshot.profile_specs.end(),
                     [&](const auto& profile) {
                         return profile.name == snapshot.default_profile;
                     }))
        return Fail(ErrorCode::INVALID_PARAMS, "unknown default profile");

    for (const auto& [medium, medium_profiles] : profiles) {
        auto pool = by_medium.find(medium);
        if (pool == by_medium.end() || pool->second.empty())
            return Fail(ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT,
                        "no eligible psegments for medium " + medium);
        for (const auto* profile : medium_profiles) {
            if (pool->second.size() < profile->member_count)
                return Fail(ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT,
                            "profile " + profile->name + " requires " +
                                std::to_string(profile->member_count) +
                                " psegments");
        }

        for (size_t profile_index = 0; profile_index < medium_profiles.size();
             ++profile_index) {
            const auto& profile = *medium_profiles[profile_index];
            std::map<std::string, PartitionPhysicalQuota> quotas;
            for (const auto& partition : ordered_partitions) {
                auto& quota = quotas[partition];
                quota.partition_id = partition;
                quota.profile_name = profile.name;
                quota.medium = medium;
            }
            for (const auto& segment : pool->second) {
                const uint64_t usable = AlignDown(
                    static_cast<uint64_t>(std::floor(
                        static_cast<long double>(segment.capacity) *
                        (1.0L - request.reserved_ratio))),
                    segment.io_alignment);
                const uint64_t lane = AlignDown(
                    usable / medium_profiles.size(), segment.io_alignment);
                const uint64_t lane_begin = lane * profile_index;
                const uint64_t per_partition = AlignDown(
                    lane / ordered_partitions.size(),
                    std::max<uint64_t>(segment.io_alignment,
                                       profile.io_alignment));
                for (size_t partition_index = 0;
                     partition_index < ordered_partitions.size();
                     ++partition_index) {
                    if (per_partition == 0) continue;
                    quotas[ordered_partitions[partition_index]].extents.push_back(
                        {segment.segment_id,
                         lane_begin + per_partition * partition_index,
                         per_partition});
                }
            }
            for (auto& [partition, quota] : quotas)
                snapshot.quotas.push_back(std::move(quota));
        }
    }
    std::string detail;
    auto validation = ValidateQuotaSnapshot(snapshot, request.segments, &detail);
    if (validation != ErrorCode::OK)
        return Fail(ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT,
                    std::move(detail));
    return {ErrorCode::OK, std::move(snapshot), {}};
}

ErrorCode EtcdPartitionQuotaSnapshotStore::Create(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    size_t max_serialized_bytes, std::string* detail) {
    std::string value;
    struct_json::to_json(snapshot, value);
    if (value.size() > max_serialized_bytes) {
        if (detail) {
            std::ostringstream stream;
            stream << "quota snapshot is " << value.size()
                   << " bytes, ETCD limit is " << max_serialized_bytes
                   << ", profiles=" << snapshot.profile_specs.size()
                   << ", quotas=" << snapshot.quotas.size();
            *detail = stream.str();
        }
        return ErrorCode::INVALID_PARAMS;
    }
    auto result = EtcdHelper::Create(key_.data(), key_.size(), value.data(),
                                     value.size());
    if (result != ErrorCode::OK && detail)
        *detail = result == ErrorCode::ETCD_TRANSACTION_FAIL
                      ? "a quota snapshot is already published"
                      : "failed to publish quota snapshot to ETCD";
    return result;
}

ErrorCode EtcdPartitionQuotaSnapshotStore::Load(
    PartitionPhysicalQuotaSnapshot* snapshot, std::string* detail) {
    if (!snapshot) return ErrorCode::INVALID_PARAMS;
    std::string value;
    EtcdRevisionId revision = 0;
    auto result = EtcdHelper::Get(key_.data(), key_.size(), value, revision);
    if (result == ErrorCode::ETCD_KEY_NOT_EXIST) {
        result = EtcdHelper::Get(
            kLegacyPartitionQuotaSnapshotKey,
            sizeof(kLegacyPartitionQuotaSnapshotKey) - 1, value, revision);
        if (result == ErrorCode::OK && detail) {
            *detail = "loaded legacy global quota snapshot; republish it "
                      "under the cluster-scoped key";
        }
    }
    if (result != ErrorCode::OK) {
        if (detail) *detail = "failed to load quota snapshot from ETCD";
        return result;
    }
    try {
        struct_json::from_json(*snapshot, value);
    } catch (const std::exception& error) {
        if (detail) *detail = std::string("invalid quota snapshot: ") + error.what();
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::vsegment
