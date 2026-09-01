#include "vchunk_metadata_store.h"

#include <utility>

#include <iomanip>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "etcd_helper.h"

namespace mooncake {
namespace {

std::string HexEncode(std::string_view value) {
    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (const unsigned char byte : value) {
        stream << std::setw(2) << static_cast<unsigned>(byte);
    }
    return stream.str();
}

tl::expected<std::vector<char>, ErrorCode> HexDecode(std::string_view value) {
    if (value.size() % 2 != 0) {
        return tl::unexpected(ErrorCode::INVALID_VERSION);
    }
    auto digit = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::vector<char> bytes;
    bytes.reserve(value.size() / 2);
    for (size_t i = 0; i < value.size(); i += 2) {
        const int high = digit(value[i]);
        const int low = digit(value[i + 1]);
        if (high < 0 || low < 0) {
            return tl::unexpected(ErrorCode::INVALID_VERSION);
        }
        bytes.push_back(static_cast<char>((high << 4) | low));
    }
    return bytes;
}

std::string MakeObjectIndexKey(std::string_view namespace_prefix,
                               const VChunkMetadataRecord& record) {
    return std::string(namespace_prefix) + "/objects/" +
           HexEncode(record.tenant_id) + "/" + HexEncode(record.key);
}

std::string MakePersistentRecordKey(std::string_view namespace_prefix,
                                    const VChunkMetadataRecord& record) {
    return std::string(namespace_prefix) + "/records/" +
           HexEncode(record.tenant_id) + "/" + record.vchunk_id;
}

std::string MakeIndexKey(std::string_view namespace_prefix,
                         const VChunkMetadataRecord& record) {
    return std::string(namespace_prefix) + "/index/" +
           HexEncode(record.tenant_id) + "/" + record.vchunk_id;
}

std::string MakePartitionKey(std::string_view namespace_prefix,
                             const VChunkMetadataRecord& record,
                             std::string_view segment_name) {
    return std::string(namespace_prefix) + "/partitions/" +
           HexEncode(record.tenant_id) + "/" + record.vchunk_id + "/" +
           HexEncode(segment_name);
}

std::string BytesToString(const std::vector<char>& bytes) {
    return {bytes.data(), bytes.size()};
}

}  // namespace

std::string MakeVChunkMetadataStoreKey(const VChunkMetadataRecord& record) {
    return std::string(kVChunkMetadataNamespace) + "/" + record.tenant_id +
           "/" + record.vchunk_id;
}

ErrorCode InMemoryVChunkMetadataStore::Put(
    const VChunkMetadataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto key = MakeVChunkMetadataStoreKey(record);
    const auto it = records_.find(key);
    if (it == records_.end()) {
        if (record.metadata_version != 0 && record.metadata_version != 1) {
            return ErrorCode::INVALID_VERSION;
        }
        records_[key] = record;
        return ErrorCode::OK;
    }
    if (record.metadata_version != it->second.metadata_version + 1) {
        return ErrorCode::INVALID_VERSION;
    }
    it->second = record;
    return ErrorCode::OK;
}

ErrorCode InMemoryVChunkMetadataStore::Remove(
    const VChunkMetadataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex_);
    records_.erase(MakeVChunkMetadataStoreKey(record));
    return ErrorCode::OK;
}

tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode>
InMemoryVChunkMetadataStore::List() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<VChunkMetadataRecord> result;
    result.reserve(records_.size());
    for (const auto& [_, record] : records_) {
        result.push_back(record);
    }
    return result;
}

EtcdVChunkMetadataStore::EtcdVChunkMetadataStore(std::string endpoints,
                                                 VChunkConfig config,
                                                 std::string cluster_id)
    : endpoints_(std::move(endpoints)),
      config_(std::move(config)),
      namespace_prefix_(std::string(kVChunkMetadataNamespace) + "/clusters/" +
                        HexEncode(cluster_id)) {
#ifdef STORE_USE_ETCD
    connection_error_ = EtcdHelper::ConnectToEtcdStoreClient(endpoints_);
#else
    connection_error_ = ErrorCode::ETCD_OPERATION_ERROR;
#endif
}

ErrorCode EtcdVChunkMetadataStore::Put(const VChunkMetadataRecord& record) {
    if (connection_error_ != ErrorCode::OK) return connection_error_;
    const auto index = BuildVChunkMetadataIndex(record);
    auto encoded_index = SerializeVChunkMetadataIndex(index, config_);
    if (!encoded_index) return encoded_index.error();
    const auto partitions = PartitionVChunkSlices(record);
    const auto metadata_key = MakeIndexKey(namespace_prefix_, record);
    const auto object_key = MakeObjectIndexKey(namespace_prefix_, record);
    const auto value = HexEncode(BytesToString(*encoded_index));
    std::vector<EtcdHelper::TxnPut> puts{{metadata_key, value}};
    puts.reserve(partitions.size() + 2);
    for (const auto& partition : partitions) {
        auto encoded = SerializeVChunkSlicePartition(partition, config_);
        if (!encoded) return encoded.error();
        puts.push_back({MakePartitionKey(namespace_prefix_, record,
                                         partition.segment_name),
                        HexEncode(BytesToString(*encoded))});
    }
    size_t put_bytes = 0;
    for (const auto& put : puts) {
        if (put.key.size() > config_.max_etcd_txn_bytes -
                                 std::min<size_t>(put_bytes,
                                                  config_.max_etcd_txn_bytes) ||
            put.value.size() >
                config_.max_etcd_txn_bytes -
                    std::min<size_t>(put_bytes + put.key.size(),
                                     config_.max_etcd_txn_bytes)) {
            return ErrorCode::INVALID_PARAMS;
        }
        put_bytes += put.key.size() + put.value.size();
    }

    if (record.status == VChunkStatus::CREATING) {
        if (record.metadata_version != 1) {
            return ErrorCode::INVALID_VERSION;
        }
        puts.push_back({object_key, record.vchunk_id});
        put_bytes += object_key.size() + record.vchunk_id.size();
        if (const auto validation = ValidateVChunkEtcdTransaction(
                puts.size() + 2, put_bytes + object_key.size() +
                                     metadata_key.size(),
                config_);
            validation != ErrorCode::OK) {
            return validation;
        }
        const auto error = EtcdHelper::TxnCompareAndPut(
            {{object_key, EtcdHelper::TxnCompareKind::kKeyNotExists, {}},
             {metadata_key, EtcdHelper::TxnCompareKind::kKeyNotExists, {}}},
            puts);
        return error == ErrorCode::ETCD_TRANSACTION_FAIL
                   ? ErrorCode::OBJECT_ALREADY_EXISTS
                   : error;
    }

    std::string current;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::Get(metadata_key.data(), metadata_key.size(),
                                 current, revision);
    if (error != ErrorCode::OK) return error;
    auto current_bytes = HexDecode(current);
    if (!current_bytes) return current_bytes.error();
    auto current_index =
        DeserializeVChunkMetadataIndex(*current_bytes, config_);
    if (!current_index) return current_index.error();
    if (record.metadata_version != current_index->metadata_version + 1) {
        return ErrorCode::INVALID_VERSION;
    }
    std::unordered_set<std::string> desired_segments;
    for (const auto& partition : partitions) {
        desired_segments.insert(partition.segment_name);
    }
    std::vector<std::string> stale_partition_keys;
    for (const auto& group : current_index->slice_groups) {
        if (!desired_segments.contains(group.segment_name)) {
            stale_partition_keys.push_back(MakePartitionKey(
                namespace_prefix_, record, group.segment_name));
        }
    }
    size_t txn_bytes = put_bytes + object_key.size() + record.vchunk_id.size() +
                       metadata_key.size() + current.size();
    for (const auto& key : stale_partition_keys) txn_bytes += key.size();
    if (const auto validation = ValidateVChunkEtcdTransaction(
            puts.size() + stale_partition_keys.size() + 2, txn_bytes,
            config_);
        validation != ErrorCode::OK) {
        return validation;
    }
    return EtcdHelper::TxnCompareAndPut(
        {{object_key, EtcdHelper::TxnCompareKind::kValueEquals,
          record.vchunk_id},
         {metadata_key, EtcdHelper::TxnCompareKind::kValueEquals, current}},
        puts, stale_partition_keys);
}

ErrorCode EtcdVChunkMetadataStore::Remove(
    const VChunkMetadataRecord& record) {
    if (connection_error_ != ErrorCode::OK) return connection_error_;
    const auto metadata_key = MakeIndexKey(namespace_prefix_, record);
    const auto object_key = MakeObjectIndexKey(namespace_prefix_, record);
    std::string current;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::Get(metadata_key.data(), metadata_key.size(),
                                 current, revision);
    if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
        const auto legacy_key =
            MakePersistentRecordKey(namespace_prefix_, record);
        error = EtcdHelper::Get(legacy_key.data(), legacy_key.size(), current,
                                revision);
        if (error == ErrorCode::ETCD_KEY_NOT_EXIST) return ErrorCode::OK;
        if (error != ErrorCode::OK) return error;
        if (const auto validation = ValidateVChunkEtcdTransaction(
                4, object_key.size() + record.vchunk_id.size() +
                       legacy_key.size() + current.size(),
                config_);
            validation != ErrorCode::OK) {
            return validation;
        }
        return EtcdHelper::TxnCompareAndPut(
            {{object_key, EtcdHelper::TxnCompareKind::kValueEquals,
              record.vchunk_id},
             {legacy_key, EtcdHelper::TxnCompareKind::kValueEquals, current}},
            {}, {legacy_key, object_key});
    }
    if (error != ErrorCode::OK) return error;
    std::vector<std::string> delete_keys{metadata_key, object_key};
    for (const auto& partition : PartitionVChunkSlices(record)) {
        delete_keys.push_back(MakePartitionKey(namespace_prefix_, record,
                                               partition.segment_name));
    }
    size_t txn_bytes = object_key.size() + record.vchunk_id.size() +
                       metadata_key.size() + current.size();
    for (const auto& key : delete_keys) txn_bytes += key.size();
    if (const auto validation = ValidateVChunkEtcdTransaction(
            delete_keys.size() + 2, txn_bytes, config_);
        validation != ErrorCode::OK) {
        return validation;
    }
    return EtcdHelper::TxnCompareAndPut(
        {{object_key, EtcdHelper::TxnCompareKind::kValueEquals,
          record.vchunk_id},
         {metadata_key, EtcdHelper::TxnCompareKind::kValueEquals, current}},
        {}, delete_keys);
}

tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode>
EtcdVChunkMetadataStore::List() {
    if (connection_error_ != ErrorCode::OK) {
        return tl::unexpected(connection_error_);
    }
    const std::string begin = namespace_prefix_ + "/";
    const std::string end = namespace_prefix_ + "0";
    std::string json;
    EtcdRevisionId revision = 0;
    auto error = EtcdHelper::GetRangeAsJson(begin.data(), begin.size(),
                                            end.data(), end.size(), 0, json,
                                            revision);
    if (error != ErrorCode::OK) return tl::unexpected(error);

    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<VChunkMetadataRecord> result;
    std::unordered_map<std::string, VChunkMetadataIndex> indexes;
    std::unordered_map<std::string, std::vector<VCSlicePartition>> partitions;
    const std::string index_prefix = begin + "index/";
    const std::string partition_prefix = begin + "partitions/";
    const std::string legacy_prefix = begin + "records/";
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        const auto key = item["key"].asString();
        auto bytes = HexDecode(item["value"].asString());
        if (!bytes) return tl::unexpected(bytes.error());
        if (key.find(index_prefix) == 0) {
            auto index = DeserializeVChunkMetadataIndex(*bytes, config_);
            if (!index) return tl::unexpected(index.error());
            indexes.emplace(key.substr(index_prefix.size()),
                            std::move(*index));
        } else if (key.find(partition_prefix) == 0) {
            auto partition =
                DeserializeVChunkSlicePartition(*bytes, config_);
            if (!partition) return tl::unexpected(partition.error());
            auto group = key.substr(partition_prefix.size());
            const auto separator = group.rfind('/');
            if (separator == std::string::npos) {
                return tl::unexpected(ErrorCode::INVALID_VERSION);
            }
            group.resize(separator);
            partitions[group].push_back(std::move(*partition));
        } else if (key.find(legacy_prefix) == 0) {
            auto record = DeserializeVChunkMetadata(*bytes, config_);
            if (!record) return tl::unexpected(record.error());
            result.push_back(std::move(*record));
        }
    }
    for (auto& [group, index] : indexes) {
        auto found = partitions.find(group);
        if (found == partitions.end()) {
            return tl::unexpected(ErrorCode::INVALID_VERSION);
        }
        auto record = AssembleVChunkMetadata(std::move(index),
                                             std::move(found->second), config_);
        if (!record) return tl::unexpected(record.error());
        result.push_back(std::move(*record));
    }
    return result;
}

ErrorCode ValidateVChunkEtcdTransaction(size_t operation_count,
                                        size_t encoded_bytes,
                                        const VChunkConfig& config) {
    return operation_count <= config.max_etcd_txn_ops &&
                   encoded_bytes <= config.max_etcd_txn_bytes
               ? ErrorCode::OK
               : ErrorCode::INVALID_PARAMS;
}

}  // namespace mooncake
