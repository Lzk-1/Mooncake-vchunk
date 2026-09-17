#include "partition/partition_router.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_keys.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "etcd_helper.h"
#include "partition/kv_hash_map.h"

namespace mooncake {
namespace partition {

void PartitionRouter::LoadSlotOwners(
    const std::vector<cvm::SlotOwner>& owners) {
    std::unordered_map<uint16_t, std::string> next;
    next.reserve(owners.size());
    for (const auto& owner : owners) {
        if (!owner.primary_master_id.empty() &&
            owner.state == static_cast<int32_t>(cvm::SlotState::kStable)) {
            next[owner.slot] = owner.primary_master_id;
        }
    }

    const size_t valid = next.size();
    {
        SharedMutexLocker locker(&mutex_);
        slot_to_submaster_ = std::move(next);
    }
    LOG(INFO) << "PartitionRouter loaded " << valid << " slot->submaster"
              << " entries (input " << owners.size() << " SlotOwner records)";
}

ErrorCode PartitionRouter::LoadFromEtcdSnapshot(
    const std::string& cluster_namespace) {
    // 本地建环（§15.4）：读 cluster_meta 得到 submaster_count，读成员列表，
    // 用与服务端完全一致的确定性算法推导 slot → master_id，无需 etcd 持久化
    // 每个 slot 的归属。
    cvm::RingMeta meta;
    ViewVersionId meta_revision = 0;
    ErrorCode err = cvm::EtcdViewStore::LoadClusterMeta(
        cluster_namespace, meta, meta_revision);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PartitionRouter load cluster_meta failed: err=" << err
                     << " (cluster may not have published ring config yet)";
        return err;
    }

    std::vector<cvm::MasterRegistration> members;
    ViewVersionId members_revision = 0;
    err = cvm::EtcdViewStore::LoadAllMasters(cluster_namespace, members,
                                             members_revision);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PartitionRouter load masters failed: err=" << err;
        return err;
    }

    // 先到先得排序（by create_revision）→ 取前 submaster_count 作为 primary_ids。
    // 与服务端 ResolveOwnedSlotsForCvm 完全一致的推导规则（不依赖 role）。
    std::sort(members.begin(), members.end(), cvm::MasterRegistrationRankLess);
    std::vector<std::string> ids;
    ids.reserve(members.size());
    for (const auto& m : members) {
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    const uint32_t submaster_count = meta.submaster_count;
    if (submaster_count > 0 && ids.size() > submaster_count) {
        ids.resize(submaster_count);
    }

    std::unordered_map<uint16_t, std::string> next;
    next.reserve(cvm::kSlotCount);
    for (uint16_t s = 0; s < cvm::kSlotCount; ++s) {
        std::string owner = cvm::ResolveSlotOwnerOnRing(ids, s);
        if (!owner.empty()) {
            next[s] = std::move(owner);
        }
    }

    const size_t valid = next.size();
    {
        SharedMutexLocker locker(&mutex_);
        slot_to_submaster_ = std::move(next);
    }
    LOG(INFO) << "PartitionRouter derived " << valid
              << " slot->submaster entries locally (primaries=" << ids.size()
              << ", submaster_count=" << submaster_count << ")";
    return ErrorCode::OK;
}

std::optional<std::string> PartitionRouter::ResolveSubmaster(
    uint16_t slot) const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto it = slot_to_submaster_.find(slot);
    if (it == slot_to_submaster_.end()) {
        LOG(WARNING) << "PartitionRouter no submaster for slot " << slot;
        return std::nullopt;
    }
    return it->second;
}

std::optional<std::string> PartitionRouter::Route(
    const TenantId& tenant, const std::string& key) const {
    return ResolveSubmaster(KvHashMap::Compute(tenant, key));
}

void PartitionRouter::Clear() {
    size_t old_size = 0;
    {
        SharedMutexLocker locker(&mutex_);
        old_size = slot_to_submaster_.size();
        slot_to_submaster_.clear();
    }
    LOG(INFO) << "PartitionRouter cleared " << old_size << " entries";
}

size_t PartitionRouter::Size() const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    return slot_to_submaster_.size();
}

}  // namespace partition
}  // namespace mooncake
