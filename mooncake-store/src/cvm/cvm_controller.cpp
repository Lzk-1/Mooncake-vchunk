#include "cvm/cvm_controller.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>
#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_http_server.h"
#include "cvm/cvm_keys.h"
#include "cvm/cvm_service_delegate.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "etcd_helper.h"

namespace mooncake {
namespace cvm {

namespace {

constexpr int kWatchEventBroken = 2;
constexpr int kWatchStopTimeoutMs = 1000;
constexpr int kKeepAliveReadyTimeoutMs = 1000;

// 逗号拼接 id 列表，空时返回 "[]"，用于 membership change 日志字段。
std::string JoinIds(const std::vector<std::string>& ids) {
    std::string out;
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i != 0) {
            out += ",";
        }
        out += ids[i];
    }
    return out.empty() ? "[]" : out;
}

}  // namespace

CvmController::CvmController(Config config) : config_(std::move(config)) {
    current_role_.store(config_.role);
}

CvmController::~CvmController() { Stop(); }

void CvmController::SetDelegate(CvmServiceDelegate* delegate) {
    delegate_ = delegate;
}

ErrorCode CvmController::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }

    LOG(INFO) << "CvmController::Start begin: cluster_namespace="
              << config_.cluster_namespace << ", master_id="
              << config_.master_id << ", address=" << config_.address
              << ", role=" << static_cast<int32_t>(config_.role)
              << ", registration_lease_ttl_sec="
              << config_.registration_lease_ttl_sec << ", http_host="
              << config_.http_host << ", http_port=" << config_.http_port
              << ", submaster_count=" << config_.submaster_count
              << ", sync_interval_ms=" << config_.sync_interval.count();

    // NOTE: the etcd client is a process-global singleton (EtcdHelper); the
    // embedding master is responsible for connecting it before Start().
    ErrorCode err = EtcdHelper::GrantLease(config_.registration_lease_ttl_sec,
                                           lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController::Start GrantLease failed: err=" << err
                   << ", cluster_namespace=" << config_.cluster_namespace
                   << ", master_id=" << config_.master_id
                   << ", registration_lease_ttl_sec="
                   << config_.registration_lease_ttl_sec
                   << " (etcd client may not be connected yet)";
        return err;
    }
    LOG(INFO) << "CvmController::Start GrantLease ok: lease_id=" << lease_id_;

    MasterRegistration reg;
    reg.master_id = config_.master_id;
    reg.address = config_.address;
    reg.role = static_cast<int32_t>(config_.role);
    reg.registered_at_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    LOG(INFO) << "CvmController::Start registering master: master_id="
              << reg.master_id << ", address=" << reg.address << ", role="
              << reg.role << ", cluster_namespace=" << config_.cluster_namespace
              << ", lease_id=" << lease_id_;
    err = EtcdViewStore::RegisterMaster(config_.cluster_namespace, reg,
                                        lease_id_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "CvmController::Start RegisterMaster failed: err=" << err
                   << ", master_id=" << reg.master_id << ", address="
                   << reg.address << ", role=" << reg.role
                   << ", cluster_namespace=" << config_.cluster_namespace
                   << ", lease_id=" << lease_id_;
        (void)EtcdHelper::RevokeLease(lease_id_);
        return err;
    }
    LOG(INFO) << "CvmController::Start RegisterMaster ok: master_id="
              << reg.master_id;

    // Persist the cluster-wide ring config (§15.3). Idempotent: submaster_count
    // is a static deployment constant, so concurrent writes converge to the
    // same value. Clients read this to locally derive the primary ring.
    RingMeta ring_meta;
    ring_meta.submaster_count = config_.submaster_count;
    err = EtcdViewStore::SaveClusterMeta(config_.cluster_namespace, ring_meta);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController::Start SaveClusterMeta failed: err="
                     << err << ", cluster_namespace=" << config_.cluster_namespace
                     << ", submaster_count=" << config_.submaster_count
                     << " (clients may fall back to stale ring until retried)";
    } else {
        LOG(INFO) << "CvmController::Start SaveClusterMeta ok: submaster_count="
                  << config_.submaster_count;
    }

    masters_watch_state_ = std::make_unique<WatchState>();
    running_.store(true);

    keepalive_thread_ = std::thread([this]() { KeepaliveLoop(); });
    masters_watch_thread_ = std::thread([this]() { MastersWatchLoop(); });
    membership_thread_ = std::thread([this]() { MembershipLoop(); });
    LOG(INFO) << "CvmController::Start started "
                 "keepalive/masters_watch/membership threads";

    if (config_.http_port != 0) {
        CvmHttpServer::Config http_cfg;
        http_cfg.host = config_.http_host;
        http_cfg.port = config_.http_port;
        http_cfg.cluster_namespace = config_.cluster_namespace;
        LOG(INFO) << "CvmController::Start starting CvmHttpServer: host="
                  << http_cfg.host << ", port=" << http_cfg.port;
        http_server_ = std::make_unique<CvmHttpServer>(http_cfg);
        err = http_server_->Start();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "CvmController start http server failed: " << err
                       << ", host=" << http_cfg.host << ", port="
                       << http_cfg.port;
            http_server_.reset();
        } else {
            LOG(INFO) << "CvmController CvmHttpServer started";
        }
    }

    LOG(INFO) << "CvmController::Start ok";
    return ErrorCode::OK;
}

void CvmController::Stop() {
    if (!running_.load() && !masters_watch_thread_.joinable() &&
        !keepalive_thread_.joinable() && !membership_thread_.joinable()) {
        return;
    }

    running_.store(false);

    CancelMastersWatchAndWait();

    if (masters_watch_state_) {
        std::lock_guard<std::mutex> lock(masters_watch_state_->mutex);
        masters_watch_state_->cv.notify_all();
    }

    if (keepalive_thread_.joinable()) {
        (void)EtcdHelper::CancelKeepAlive(lease_id_);
        keepalive_thread_.join();
    }
    if (masters_watch_thread_.joinable()) {
        masters_watch_thread_.join();
    }
    if (membership_thread_.joinable()) {
        membership_cv_.notify_all();
        membership_thread_.join();
    }

    if (http_server_) {
        http_server_->Stop();
        http_server_.reset();
    }

    (void)EtcdHelper::RevokeLease(lease_id_);
    masters_watch_state_.reset();
    LOG(INFO) << "CvmController::Stop ok: master_id=" << config_.master_id
              << ", lease_id=" << lease_id_;
}

void CvmController::CancelMastersWatchAndWait() {
    if (masters_watch_armed_.exchange(false)) {
        (void)EtcdViewStore::CancelWatchMasters(config_.cluster_namespace);
        (void)EtcdViewStore::WaitWatchMastersStopped(config_.cluster_namespace,
                                                     kWatchStopTimeoutMs);
    }
}

void CvmController::MastersWatchLoop() {
    while (running_.load()) {
        {
            std::lock_guard<std::mutex> lock(masters_watch_state_->mutex);
            masters_watch_state_->dirty = false;
            masters_watch_state_->broken = false;
        }

        // start_revision=0 → 从当前开始 watch，只关注未来的成员增删（lease
        // 过期删除 / 新节点注册）。
        ErrorCode err = EtcdViewStore::WatchMasters(
            config_.cluster_namespace, /*start_revision=*/0,
            masters_watch_state_.get(), &CvmController::WatchCallback);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmController arm masters watch failed: " << err;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        masters_watch_armed_.store(true);

        // Go 侧 watch 是持久 watch（一次注册，持续派发事件），普通事件后
        // 不会自动注销；仅在真正断开（broken）时才需要重新 arm。因此用内层
        // 循环等待事件：普通事件清 dirty 后继续等，绝不重复注册同前缀——
        // 否则会命中 "already being watched" 并以 1s 间隔刷屏。
        while (running_.load()) {
            std::unique_lock<std::mutex> lock(masters_watch_state_->mutex);
            masters_watch_state_->cv.wait(lock, [this] {
                return masters_watch_state_->dirty || !running_.load();
            });
            if (!running_.load()) {
                break;
            }

            const bool broken = masters_watch_state_->broken;
            masters_watch_state_->dirty = false;
            masters_watch_state_->broken = false;
            lock.unlock();

            // 成员增删（lease 过期）→ 先探测成员集变化并记录日志，再重算角色。
            std::vector<MasterRegistration> members;
            if (LoadRankedMembers(members)) {
                LogMembershipChange(members);
            }
            ReconcileRole();
            // 成员集变化：即使本机角色不变（仍是 standby），也要通知 delegate
            // 重新从最新成员列表本地推导回放源（替代已删除的 kv_view watch）。
            if (delegate_) {
                delegate_->OnMembershipChanged();
            }

            if (broken) {
                // watch 已断开（goroutine 已自清理并退出），退出内层循环
                // 重新 arm；普通事件则继续等待下一个事件。
                break;
            }
        }
    }
}

void CvmController::KeepaliveLoop() {
    (void)EtcdHelper::WaitKeepAliveReady(lease_id_, kKeepAliveReadyTimeoutMs);
    ErrorCode rc = EtcdHelper::KeepAlive(lease_id_);
    if (rc == ErrorCode::ETCD_OPERATION_ERROR) {
        LOG(WARNING) << "CvmController keepalive error: " << rc;
    }
}

bool CvmController::LoadRankedMembers(
    std::vector<MasterRegistration>& out) {
    std::vector<MasterRegistration> masters;
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::LoadAllMasters(config_.cluster_namespace,
                                                  masters, version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmController load masters failed: " << err
                     << ", master_id=" << config_.master_id;
        return false;
    }

    // 收集所有存活 master（含本机）作为候选，按「先到先得」排序：etcd
    // create_revision 升序（谁先注册谁排前），revision 缺失时回退 master_id。
    // create_revision 由 etcd 全局单调分配，不存在本地时钟偏差，节点/客户端
    // 可字节级一致地推导出相同 primary 序列（见 MasterRegistrationRankLess）。
    out.clear();
    out.reserve(masters.size());
    for (const auto& m : masters) {
        if (!m.master_id.empty()) {
            out.push_back(m);
        }
    }

    std::sort(out.begin(), out.end(), MasterRegistrationRankLess);
    return true;
}

void CvmController::LogMembershipChange(
    const std::vector<MasterRegistration>& members) {
    std::vector<std::string> ids;
    ids.reserve(members.size());
    for (const auto& m : members) {
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    // LoadRankedMembers 已按 create_revision 排序，ids 继承该顺序。成员集未变则不打
    // 日志（watch 可能因等值事件重复唤醒）。
    if (ids == last_member_ids_) {
        return;
    }

    std::set<std::string> prev(last_member_ids_.begin(), last_member_ids_.end());
    std::set<std::string> cur(ids.begin(), ids.end());
    std::vector<std::string> joined;
    std::vector<std::string> left;
    for (const auto& id : ids) {
        if (!prev.count(id)) {
            joined.push_back(id);
        }
    }
    for (const auto& id : last_member_ids_) {
        if (!cur.count(id)) {
            left.push_back(id);
        }
    }

    const size_t primary_count =
        std::min<size_t>(ids.size(), config_.submaster_count);
    std::vector<std::string> primaries(ids.begin(),
                                       ids.begin() + primary_count);

    LOG(INFO) << "CvmController membership change: master_id="
              << config_.master_id << ", joined=" << JoinIds(joined)
              << ", left=" << JoinIds(left)
              << ", primaries=" << JoinIds(primaries);

    last_member_ids_ = std::move(ids);
}

MasterRole CvmController::ComputeDesiredRole() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members)) {
        return current_role_.load();
    }

    for (size_t i = 0; i < members.size(); ++i) {
        if (members[i].master_id == config_.master_id) {
            return i < config_.submaster_count ? MasterRole::kPrimary
                                               : MasterRole::kStandby;
        }
    }

    // 本机不在成员集（未注册/异常）：保守保持当前角色。
    return current_role_.load();
}

std::string CvmController::GetPrimaryAddress() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return "";
    }
    // 排名第一的成员是集群中最早的 primary。若本机就是它，则无需（也不能）
    // 把自己当作回放源，返回空字符串让调用方保持纯 standby。
    if (members.front().master_id == config_.master_id) {
        return "";
    }
    return members.front().address;
}

std::vector<MasterRegistration> CvmController::GetBindingSources() {
    std::vector<MasterRegistration> members;
    if (!LoadRankedMembers(members) || members.empty()) {
        return {};
    }

    const size_t primary_count =
        std::min<size_t>(members.size(), config_.submaster_count);

    // 本机在「先到先得」（create_revision 稳定排序）列表中的位置。
    size_t my_index = members.size();
    for (size_t i = 0; i < members.size(); ++i) {
        if (members[i].master_id == config_.master_id) {
            my_index = i;
            break;
        }
    }
    // 未注册 / 本机已是 primary：不作为 standby 回放。
    if (my_index == members.size() || my_index < primary_count) {
        return {};
    }

    const size_t standby_count = members.size() - primary_count;
    if (standby_count == 0) {
        return {};
    }
    const size_t standby_rank = my_index - primary_count;

    // 本 standby 负责的 slot 区间 [start, end)，与其它 standby 均分 16384。
    const uint16_t start = static_cast<uint16_t>(standby_rank * kSlotCount /
                                                 standby_count);
    const uint16_t end = static_cast<uint16_t>((standby_rank + 1) * kSlotCount /
                                               standby_count);

    // 本地一致性哈希环：primary_ids = 排序去重后的前 submaster_count 个成员，
    // 与服务端/客户端完全一致（slot_hash.h）。
    std::vector<std::string> primary_ids;
    primary_ids.reserve(primary_count);
    for (size_t i = 0; i < primary_count; ++i) {
        primary_ids.push_back(members[i].master_id);
    }

    // 求出负责区间内每个 slot 的 primary owner（确定性推导）。
    std::set<std::string> owner_ids;
    for (uint16_t slot = start; slot < end; ++slot) {
        const std::string owner = ResolveSlotOwnerOnRing(primary_ids, slot);
        if (!owner.empty() && owner != config_.master_id) {
            owner_ids.insert(owner);
        }
    }

    // 映射 owner master_id -> MasterRegistration（含 address）。
    std::vector<MasterRegistration> sources;
    sources.reserve(owner_ids.size());
    for (const auto& m : members) {
        if (owner_ids.count(m.master_id)) {
            sources.push_back(m);
        }
    }
    if (!sources.empty()) {
        LOG(INFO) << "CvmController standby binding done: master_id="
                  << config_.master_id << ", slot_range=[" << start << ","
                  << end << "), source_count=" << sources.size();
    }
    return sources;
}

void CvmController::ReconcileRole() {
    const MasterRole desired = ComputeDesiredRole();
    MasterRole current = current_role_.load();
    if (desired == current) {
        return;
    }
    // CAS：membership_thread_ 与 masters_watch_thread_ 并发调用时，仅一个
    // 线程执行角色迁移与通知，避免重复 OnRoleChanged。
    if (!current_role_.compare_exchange_strong(current, desired)) {
        return;
    }
    LOG(INFO) << "CvmController role decision: master_id="
              << config_.master_id << ", current="
              << static_cast<int32_t>(current)
              << ", desired=" << static_cast<int32_t>(desired)
              << ", submaster_count=" << config_.submaster_count;
    if (delegate_) {
        delegate_->OnRoleChanged(desired);
    }
}

void CvmController::MembershipLoop() {
    while (running_.load()) {
        ReconcileRole();

        std::unique_lock<std::mutex> lock(membership_mutex_);
        membership_cv_.wait_for(lock, config_.sync_interval,
                                [this] { return !running_.load(); });
    }
}

void CvmController::WatchCallback(void* ctx, const char* /*key*/,
                                  size_t /*key_size*/, const char* /*value*/,
                                  size_t /*value_size*/, int event_type,
                                  int64_t /*mod_revision*/) {
    auto* state = static_cast<WatchState*>(ctx);
    if (state == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->dirty = true;
        if (event_type == kWatchEventBroken) {
            state->broken = true;
        }
    }
    state->cv.notify_all();
}

}  // namespace cvm
}  // namespace mooncake