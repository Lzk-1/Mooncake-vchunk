#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cvm/cvm_types.h"
#include "mutex.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace partition {

// client 侧路由：把逻辑 slot 解析为 submaster_id（即 master_id，其值等于
// RPC 端点 address）。映射来源为本地确定性哈希环推导（读成员列表 +
// cluster_meta 的 submaster_count），与服务端 ResolveOwnedSlotsForCvm 一致，
// 不再从 etcd 快照读逐 slot 归属。
class PartitionRouter {
   public:
    // 加载 slot → submaster 映射（覆盖式）。仅用于单元测试 / 兼容旧路径。
    void LoadSlotOwners(const std::vector<cvm::SlotOwner>& owners);

    // 读成员列表 + cluster_meta，本地建环并刷新映射。
    ErrorCode LoadFromEtcdSnapshot(const std::string& cluster_namespace);

    // slot → submaster_id（primary_master_id）；未命中返回 nullopt。
    std::optional<std::string> ResolveSubmaster(uint16_t slot) const;

    // key → submaster_id（先哈希再路由）；未命中返回 nullopt。
    std::optional<std::string> Route(const TenantId& tenant,
                                     const std::string& key) const;

    void Clear();
    size_t Size() const;

   private:
    mutable SharedMutex mutex_;
    std::unordered_map<uint16_t, std::string> slot_to_submaster_;
};

}  // namespace partition
}  // namespace mooncake
