#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct VChunkSubMasterMember {
    std::string submaster_id;
    YLT_REFL(VChunkSubMasterMember, submaster_id);
};

class EtcdVChunkMembership {
   public:
    EtcdVChunkMembership(std::string endpoints, std::string cluster_id);
    ~EtcdVChunkMembership();

    EtcdVChunkMembership(const EtcdVChunkMembership&) = delete;
    EtcdVChunkMembership& operator=(const EtcdVChunkMembership&) = delete;

    ErrorCode Start(std::string submaster_id, uint32_t ttl_seconds);
    void Stop();
    tl::expected<std::vector<VChunkSubMasterMember>, ErrorCode> List() const;

    bool Healthy() const { return healthy_.load(); }
    ErrorCode LastError() const { return last_error_.load(); }

   private:
    std::string MemberPrefix() const;

    std::string endpoints_;
    std::string cluster_id_;
    std::string member_key_;
    EtcdLeaseId lease_id_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> healthy_{false};
    std::atomic<ErrorCode> last_error_{ErrorCode::OK};
    mutable std::mutex mutex_;
    std::thread keepalive_thread_;
};

}  // namespace mooncake
