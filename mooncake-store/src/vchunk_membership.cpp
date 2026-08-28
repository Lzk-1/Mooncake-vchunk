#include "vchunk_membership.h"

#include <sstream>
#include <utility>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "etcd_helper.h"

namespace mooncake {

EtcdVChunkMembership::EtcdVChunkMembership(std::string endpoints,
                                           std::string cluster_id)
    : endpoints_(std::move(endpoints)), cluster_id_(std::move(cluster_id)) {}

EtcdVChunkMembership::~EtcdVChunkMembership() { Stop(); }

std::string EtcdVChunkMembership::MemberPrefix() const {
    return "/mooncake/vchunk/v1/clusters/" + cluster_id_ + "/members/";
}

ErrorCode EtcdVChunkMembership::Start(std::string submaster_id,
                                      uint32_t ttl_seconds) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (running_.load() || endpoints_.empty() || cluster_id_.empty() ||
        cluster_id_.find('/') != std::string::npos || submaster_id.empty() ||
        submaster_id.find('/') != std::string::npos || ttl_seconds < 3) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (keepalive_thread_.joinable()) keepalive_thread_.join();
#ifndef STORE_USE_ETCD
    return ErrorCode::ETCD_OPERATION_ERROR;
#else
    auto error = EtcdHelper::ConnectToEtcdStoreClient(endpoints_);
    if (error != ErrorCode::OK) return error;
    error = EtcdHelper::GrantLease(ttl_seconds, lease_id_);
    if (error != ErrorCode::OK) return error;
    member_key_ = MemberPrefix() + submaster_id;
    EtcdRevisionId revision = 0;
    error = EtcdHelper::CreateWithLease(
        member_key_.data(), member_key_.size(), submaster_id.data(),
        submaster_id.size(), lease_id_, revision);
    if (error != ErrorCode::OK) {
        (void)EtcdHelper::RevokeLease(lease_id_);
        lease_id_ = 0;
        return error == ErrorCode::ETCD_TRANSACTION_FAIL
                   ? ErrorCode::OBJECT_ALREADY_EXISTS
                   : error;
    }
    running_.store(true);
    healthy_.store(true);
    last_error_.store(ErrorCode::OK);
    keepalive_thread_ = std::thread([this] {
        const auto result = EtcdHelper::KeepAlive(lease_id_);
        last_error_.store(result);
        healthy_.store(false);
        running_.store(false);
    });
    error = EtcdHelper::WaitKeepAliveReady(lease_id_, 5000);
    if (error != ErrorCode::OK) {
        (void)EtcdHelper::CancelKeepAlive(lease_id_);
        if (keepalive_thread_.joinable()) keepalive_thread_.join();
        (void)EtcdHelper::RevokeLease(lease_id_);
        lease_id_ = 0;
        healthy_.store(false);
        last_error_.store(error);
        return error;
    }
    return ErrorCode::OK;
#endif
}

void EtcdVChunkMembership::Stop() {
    std::lock_guard<std::mutex> guard(mutex_);
#ifdef STORE_USE_ETCD
    if (lease_id_ != 0) {
        (void)EtcdHelper::CancelKeepAlive(lease_id_);
    }
#endif
    if (keepalive_thread_.joinable()) keepalive_thread_.join();
#ifdef STORE_USE_ETCD
    if (lease_id_ != 0) (void)EtcdHelper::RevokeLease(lease_id_);
#endif
    lease_id_ = 0;
    member_key_.clear();
    running_.store(false);
    healthy_.store(false);
}

tl::expected<std::vector<VChunkSubMasterMember>, ErrorCode>
EtcdVChunkMembership::List() const {
#ifndef STORE_USE_ETCD
    return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
#else
    if (endpoints_.empty() || cluster_id_.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto prefix = MemberPrefix();
    const auto end = prefix + "0";
    std::string json;
    EtcdRevisionId revision = 0;
    const auto error = EtcdHelper::GetRangeAsJson(
        prefix.data(), prefix.size(), end.data(), end.size(), 0, json,
        revision);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<VChunkSubMasterMember> result;
    result.reserve(root.size());
    for (const auto& item : root) {
        if (!item.isObject() || !item["value"].isString()) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        auto id = item["value"].asString();
        if (id.empty()) return tl::make_unexpected(ErrorCode::INVALID_VERSION);
        result.push_back({std::move(id)});
    }
    return result;
#endif
}

}  // namespace mooncake
