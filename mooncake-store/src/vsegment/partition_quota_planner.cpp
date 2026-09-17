#include "vsegment/partition_quota_planner.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

#include "etcd_helper.h"
#include "crc32c.h"
#include "cvm/slot_hash.h"

namespace mooncake::vsegment {
namespace {

PartitionQuotaPlanResult Fail(ErrorCode error, std::string detail) {
    return {error, {}, std::move(detail)};
}

uint64_t AlignDown(uint64_t value, uint64_t alignment) {
    return value - value % alignment;
}

}  // namespace

ErrorCode BuildDiscoveredQuotaPlan(
    const VSegmentUserPolicy& policy,
    const std::vector<cvm::SegmentDescriptor>& descriptors,
    const std::vector<cvm::MasterRegistration>& masters,
    const std::vector<std::pair<std::string, cvm::MountEntry>>& mounts,
    PartitionQuotaPlanRequest* request, std::string* detail,
    bool allow_non_exclusive) {
    if (!request) return ErrorCode::INVALID_PARAMS;
    if (policy.member_count == 0 || policy.stripe_size == 0 ||
        policy.member_extent_size == 0) {
        if (detail)
            *detail = "member_count, stripe_size and member_extent_size are "
                      "required user policy parameters";
        return ErrorCode::INVALID_PARAMS;
    }
    if (policy.reserved_ratio < 0.0 || policy.reserved_ratio >= 1.0) {
        if (detail) *detail = "reserved_ratio must be in [0, 1)";
        return ErrorCode::INVALID_PARAMS;
    }
    if (policy.default_profile.empty() || policy.profile_name.empty() ||
        policy.required_medium.empty()) {
        if (detail)
            *detail = "default_profile, profile_name and required_medium "
                      "must not be empty";
        return ErrorCode::INVALID_PARAMS;
    }

    PartitionQuotaPlanRequest next;
    next.config_generation = policy.config_generation;
    next.default_profile = policy.default_profile;
    next.reserved_ratio = policy.reserved_ratio;
    next.profile_specs = {{policy.profile_name, policy.member_count,
                           policy.stripe_size, policy.member_extent_size,
                           policy.io_alignment, policy.required_medium,
                           policy.initial_vsegment_count}};
    auto error = ValidateProfile(next.profile_specs.front(), detail);
    if (error != ErrorCode::OK) return error;

    // 在线 master 集合（用于过滤 mount 记录：仅 live master 的 mount 才算
    // 该 psegment 在线）。
    std::set<std::string> live_masters;
    for (const auto& master : masters)
        if (!master.master_id.empty()) live_masters.insert(master.master_id);
    std::set<std::string> mounted;
    for (const auto& [master, mount] : mounts)
        if (live_masters.count(master)) mounted.insert(mount.segment_id);

    // 从 SegmentDescriptor 自动发现资源事实（介质/对齐/对齐能力/故障域），
    // 不再硬编码。同时校验 vsegment_exclusive：自动发现「总容量」≠「空闲
    // 空间」，仅 vsegment_exclusive=true 的 psegment 才可按 capacity 切分。
    std::set<std::string> seen;
    std::vector<std::string> rejected_non_exclusive;
    for (const auto& desc : descriptors) {
        if (desc.segment_id.empty() || !seen.insert(desc.segment_id).second) {
            if (detail) *detail = "empty or duplicate CVM segment id";
            return ErrorCode::INVALID_PARAMS;
        }
        if (!mounted.count(desc.segment_id) || desc.capacity == 0 ||
            desc.te_endpoint.empty())
            continue;  // 离线或无效，跳过
        if (!desc.vsegment_exclusive) {
            rejected_non_exclusive.push_back(desc.segment_id);
            continue;
        }
        if (desc.medium.empty()) {
            if (detail) {
                *detail = "segment " + desc.segment_id +
                          " has empty medium; allocator must report resource "
                          "facts at mount time";
            }
            return ErrorCode::INVALID_PARAMS;
        }
        // failure_domain 缺省由 host_id 兜底，仍为空时用 segment_id 兜底，
        // 保证 PSegmentGeometry.failure_domain 非空（便于后续故障域隔离）。
        std::string failure_domain = desc.failure_domain;
        if (failure_domain.empty())
            failure_domain = desc.host_id.empty() ? desc.segment_id
                                                  : desc.host_id;
        next.segments.push_back({desc.segment_id, desc.capacity,
                                 desc.io_alignment == 0 ? 1 : desc.io_alignment,
                                 desc.medium, /*healthy=*/true,
                                 desc.supports_unaligned_io, failure_domain});
    }
    if (!rejected_non_exclusive.empty() && !allow_non_exclusive) {
        if (detail) {
            std::ostringstream os;
            os << "automatic discovery refuses to treat total capacity as "
                  "free space. The following psegments are not declared "
                  "vsegment_exclusive and may be in use by other "
                  "allocators; reserve their allocatable ranges via the "
                  "space management mechanism first, or set "
                  "cvm_segments_vsegment_exclusive=true on a cluster "
                  "dedicated to vsegment at init time. Rejected: ";
            for (size_t i = 0; i < rejected_non_exclusive.size(); ++i) {
                if (i) os << ",";
                os << rejected_non_exclusive[i];
            }
            os << " (pass --allow_non_exclusive only if you have confirmed "
                   "allocatable ranges out of band)";
            *detail = os.str();
        }
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    }
    if (next.segments.size() < policy.member_count) {
        if (detail) {
            std::ostringstream os;
            os << "insufficient live mounted vsegment_exclusive segments for "
                  "member_count="
               << policy.member_count
               << ", discovered=" << next.segments.size();
            if (!rejected_non_exclusive.empty()) {
                os << " (additionally " << rejected_non_exclusive.size()
                   << " non-exclusive segments rejected)";
            }
            *detail = os.str();
        }
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    }
    std::sort(next.segments.begin(), next.segments.end(),
              [](const auto& a, const auto& b) {
                  return a.segment_id < b.segment_id;
              });
    // Partition 列表由 KV PT 生成（确定性哈希方案，slot 即 partition）。
    for (uint16_t slot = 0; slot < cvm::kSlotCount; ++slot)
        next.partition_ids.push_back(std::to_string(slot));
    std::string serialized;
    struct_json::to_json(next, serialized);
    Crc32c crc;
    crc.Extend(serialized.data(), serialized.size());
    next.policy_digest = "plan-crc32c-" + std::to_string(crc.Final());
    *request = std::move(next);
    return ErrorCode::OK;
}

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

    // A physical byte can belong to only one profile. Balanced mode therefore
    // requires profile pools to be disjoint; medium is the available hard
    // selector in the current psegment inventory model.
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
        if (medium_profiles.size() != 1) {
            return Fail(ErrorCode::INVALID_PARAMS,
                        "balanced profiles overlap psegment medium " + medium);
        }
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

        for (const auto* profile_ptr : medium_profiles) {
            const auto& profile = *profile_ptr;
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
                const uint64_t per_partition = AlignDown(
                    usable / ordered_partitions.size(),
                    std::max<uint64_t>(segment.io_alignment,
                                       profile.io_alignment));
                for (size_t partition_index = 0;
                     partition_index < ordered_partitions.size();
                     ++partition_index) {
                    if (per_partition == 0) continue;
                    quotas[ordered_partitions[partition_index]].extents.push_back(
                        {segment.segment_id,
                         per_partition * partition_index,
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
