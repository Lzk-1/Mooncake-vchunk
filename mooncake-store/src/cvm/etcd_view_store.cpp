#include "cvm/etcd_view_store.h"

#include <sstream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "cvm/cvm_keys.h"
#include "etcd_helper.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {
namespace cvm {

namespace {

// Parses the JSON array returned by EtcdHelper::GetRangeAsJson, which has the
// form [{"key":"...","value":"..."}, ...].
ErrorCode ParseRangeJson(const std::string& json,
                         std::vector<std::pair<std::string, std::string>>& kvs) {
    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        LOG(ERROR) << "Failed to parse etcd range JSON: " << errors;
        return ErrorCode::INTERNAL_ERROR;
    }

    kvs.clear();
    kvs.reserve(root.size());
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        kvs.emplace_back(item["key"].asString(), item["value"].asString());
    }
    return ErrorCode::OK;
}

// key/value 之外额外解析 etcd key create_revision，用于「先到先得」的 primary
// 排序（MasterRegistrationRankLess）。create_revision 缺失时记为 0，由其
// 排序比较器回退 master_id 兜底。
struct RangeKvWithRevision {
    std::string key;
    std::string value;
    int64_t create_revision{0};
};

ErrorCode ParseRangeJsonWithRevision(
    const std::string& json, std::vector<RangeKvWithRevision>& kvs) {
    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        LOG(ERROR) << "Failed to parse etcd range JSON: " << errors;
        return ErrorCode::INTERNAL_ERROR;
    }

    kvs.clear();
    kvs.reserve(root.size());
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        RangeKvWithRevision kv;
        kv.key = item["key"].asString();
        kv.value = item["value"].asString();
        if (item.isMember("create_revision") && item["create_revision"].isNumeric()) {
            kv.create_revision = item["create_revision"].asInt64();
        }
        kvs.push_back(std::move(kv));
    }
    return ErrorCode::OK;
}

}  // namespace

// ---- JSON serialization ----

ErrorCode EtcdViewStore::SerializeMasterRegistration(const MasterRegistration& reg,
                                                     std::string& out) {
    try {
        struct_json::to_json(reg, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeMasterRegistration failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeMasterRegistration(const std::string& in,
                                                       MasterRegistration& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeMasterRegistration failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializeRingMeta(const RingMeta& meta,
                                           std::string& out) {
    try {
        struct_json::to_json(meta, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeRingMeta failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeRingMeta(const std::string& in,
                                             RingMeta& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeRingMeta failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializePartitionRoute(
    const partition::PartitionRoute& route, std::string& out) {
    try {
        struct_json::to_json(route, out);
        return ErrorCode::OK;
    } catch (...) {
        return ErrorCode::SERIALIZE_FAIL;
    }
}

ErrorCode EtcdViewStore::DeserializePartitionRoute(
    const std::string& in, partition::PartitionRoute& out) {
    try {
        struct_json::from_json(out, in);
        return ErrorCode::OK;
    } catch (...) {
        return ErrorCode::DESERIALIZE_FAIL;
    }
}

ErrorCode EtcdViewStore::SavePartitionRoute(
    const std::string& ns, const partition::PartitionRoute& route) {
    if (route.partition_id.partition_id.empty() ||
        route.owner_submaster_id.empty() ||
        route.route_epoch == 0)
        return ErrorCode::INVALID_PARAMS;
    std::string value;
    auto error = SerializePartitionRoute(route, value);
    if (error != ErrorCode::OK) return error;
    const auto key =
        PartitionRouteKey(ns, route.partition_id.partition_id);
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::LoadPartitionRoute(
    const std::string& ns, const std::string& partition_id,
    partition::PartitionRoute& out, ViewVersionId& version) {
    const auto key = PartitionRouteKey(ns, partition_id);
    std::string value;
    auto error = EtcdHelper::Get(key.data(), key.size(), value, version);
    return error == ErrorCode::OK ? DeserializePartitionRoute(value, out)
                                  : error;
}

ErrorCode EtcdViewStore::CASSwitchPartitionOwner(
    const std::string& ns, const std::string& partition_id,
    uint64_t expected_epoch, const std::string& new_owner,
    partition::PartitionState state, const std::string& target,
    partition::PartitionRoute& out) {
    partition::PartitionRoute current;
    ViewVersionId version = 0;
    auto error = LoadPartitionRoute(ns, partition_id, current, version);
    if (error != ErrorCode::OK) return error;
    if (current.route_epoch != expected_epoch) return ErrorCode::STALE_ROUTE;
    std::string old_value;
    SerializePartitionRoute(current, old_value);
    out = {{partition_id}, new_owner, expected_epoch + 1,
           static_cast<int32_t>(state), target};
    std::string new_value;
    SerializePartitionRoute(out, new_value);
    const auto key = PartitionRouteKey(ns, partition_id);
    error = EtcdHelper::TxnCompareAndPut(
        {{key, EtcdHelper::TxnCompareKind::kValueEquals, old_value}},
        {{key, new_value}});
    return error == ErrorCode::ETCD_TRANSACTION_FAIL ? ErrorCode::STALE_ROUTE
                                                      : error;
}


// ---- Segment neutral entity (segments/{segment_id}) ----

ErrorCode EtcdViewStore::SerializeSegmentDescriptor(
    const SegmentDescriptor& desc, std::string& out) {
    try {
        struct_json::to_json(desc, out);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeSegmentDescriptor failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::DeserializeSegmentDescriptor(
    const std::string& in, SegmentDescriptor& out) {
    try {
        struct_json::from_json(out, in);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeSegmentDescriptor failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::SaveSegmentDescriptor(
    const std::string& cluster_namespace, const SegmentDescriptor& desc) {
    const std::string key =
        SegmentNeutralEntityKey(cluster_namespace, desc.segment_id);
    std::string value;
    ErrorCode err = SerializeSegmentDescriptor(desc, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::DeleteSegmentDescriptor(
    const std::string& cluster_namespace, const std::string& segment_id) {
    const std::string key =
        SegmentNeutralEntityKey(cluster_namespace, segment_id);
    const std::string end = PrefixEnd(key);
    return EtcdHelper::DeleteRange(key.data(), key.size(), end.data(),
                                   end.size());
}

ErrorCode EtcdViewStore::LoadAllSegmentDescriptors(
    const std::string& cluster_namespace,
    std::vector<SegmentDescriptor>& out, ViewVersionId& version) {
    out.clear();
    const std::string prefix = SegmentNeutralEntityPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        SegmentDescriptor desc;
        err = DeserializeSegmentDescriptor(kv.second, desc);
        if (err != ErrorCode::OK) {
            return err;
        }
        out.push_back(std::move(desc));
    }
    return ErrorCode::OK;
}

// ---- Per-master segment mount (snapshot/{id}/segments/{seg}) ----

ErrorCode EtcdViewStore::SerializeMountEntry(const MountEntry& entry,
                                             std::string& out) {
    try {
        struct_json::to_json(entry, out);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeMountEntry failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::DeserializeMountEntry(const std::string& in,
                                               MountEntry& out) {
    try {
        struct_json::from_json(out, in);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeMountEntry failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::SaveMountEntryWithLease(
    const std::string& cluster_namespace, const std::string& master_id,
    const MountEntry& entry, EtcdLeaseId lease_id) {
    const std::string key =
        SnapshotSegmentMountKey(cluster_namespace, master_id, entry.segment_id);
    std::string value;
    ErrorCode err = SerializeMountEntry(entry, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::DeleteMountEntry(
    const std::string& cluster_namespace, const std::string& master_id,
    const std::string& segment_id) {
    const std::string key =
        SnapshotSegmentMountKey(cluster_namespace, master_id, segment_id);
    const std::string end = PrefixEnd(key);
    return EtcdHelper::DeleteRange(key.data(), key.size(), end.data(),
                                   end.size());
}

ErrorCode EtcdViewStore::LoadAllMountEntries(
    const std::string& cluster_namespace,
    std::vector<std::pair<std::string, MountEntry>>& out,
    ViewVersionId& version) {
    out.clear();
    const std::string prefix = SnapshotPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    // key = snapshot/{master_id}/segments/{segment_id}. Skip any other
    // snapshot subkey (e.g. future reg/oplog) that may appear in the scan.
    constexpr char kSegmentsMarker[] = "/segments/";
    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        const std::size_t pos = kv.first.rfind(kSegmentsMarker);
        if (pos == std::string::npos || pos <= prefix.size()) {
            continue;
        }
        MountEntry entry;
        err = DeserializeMountEntry(kv.second, entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        const std::string master_id =
            kv.first.substr(prefix.size(), pos - prefix.size());
        out.emplace_back(master_id, std::move(entry));
    }
    return ErrorCode::OK;
}

// ---- Master registration ----

ErrorCode EtcdViewStore::RegisterMaster(const std::string& cluster_namespace,
                                        const MasterRegistration& reg,
                                        EtcdLeaseId lease_id) {
    const std::string key =
        MasterRegistrationKey(cluster_namespace, reg.master_id);
    std::string value;
    ErrorCode err = SerializeMasterRegistration(reg, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::UpdateMasterRole(const std::string& cluster_namespace,
                                          const std::string& master_id,
                                          MasterRole role,
                                          EtcdLeaseId lease_id) {
    const std::string key = MasterRegistrationKey(cluster_namespace, master_id);

    MasterRegistration reg;
    ViewVersionId version = 0;
    std::string existing;
    ErrorCode err =
        EtcdHelper::Get(key.data(), key.size(), existing, version);
    if (err == ErrorCode::OK) {
        err = DeserializeMasterRegistration(existing, reg);
        if (err != ErrorCode::OK) {
            return err;
        }
    }
    // On read failure (e.g. key missing) fall back to a minimal registration;
    // the caller only flips role on a previously-registered master, so this
    // path is defensive and preserves liveness via the lease.
    reg.master_id = master_id;
    reg.role = static_cast<int32_t>(role);

    std::string value;
    err = SerializeMasterRegistration(reg, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::LoadAllMasters(
    const std::string& cluster_namespace, std::vector<MasterRegistration>& out,
    ViewVersionId& version) {
    out.clear();
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<RangeKvWithRevision> kvs;
    err = ParseRangeJsonWithRevision(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        MasterRegistration reg;
        err = DeserializeMasterRegistration(kv.value, reg);
        if (err != ErrorCode::OK) {
            return err;
        }
        reg.create_revision = kv.create_revision;
        out.push_back(std::move(reg));
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SaveClusterMeta(const std::string& cluster_namespace,
                                         const RingMeta& meta) {
    const std::string key = ClusterMetaKey(cluster_namespace);
    std::string value;
    ErrorCode err = SerializeRingMeta(meta, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::LoadClusterMeta(const std::string& cluster_namespace,
                                         RingMeta& out,
                                         ViewVersionId& version) {
    const std::string key = ClusterMetaKey(cluster_namespace);
    std::string value;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    return DeserializeRingMeta(value, out);
}

// ---- Master membership watch ----

ErrorCode EtcdViewStore::WatchMasters(const std::string& cluster_namespace,
                                      ViewVersionId start_revision, void* ctx,
                                      WatchCallback cb) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::WatchWithPrefixFromRevision(prefix.data(), prefix.size(),
                                                   start_revision, ctx, cb);
}

ErrorCode EtcdViewStore::CancelWatchMasters(
    const std::string& cluster_namespace) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::CancelWatchWithPrefix(prefix.data(), prefix.size());
}

ErrorCode EtcdViewStore::WaitWatchMastersStopped(
    const std::string& cluster_namespace, int timeout_ms) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::WaitWatchWithPrefixStopped(prefix.data(), prefix.size(),
                                                  timeout_ms);
}

}  // namespace cvm
}  // namespace mooncake
