#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cvm/cvm_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

class CvmServiceDelegate;
class CvmHttpServer;

// In-process control plane for the CVM (Cache View Master).
//
// Responsibilities:
//   - Register this master under an etcd lease (liveness + role).
//   - Persist the cluster-wide ring config (cluster_meta) once at startup.
//   - Coordinate the submaster quota (first-come-first-served ranking) and
//     publish role changes to the delegate.
//
// Slot ownership is derived deterministically from the primary member list
// (see slot_hash.h), so no per-slot ownership view is cached or watched here.
class CvmController {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        std::string address;  // RPC endpoint of this master.
        MasterRole role = MasterRole::kPrimary;
        int64_t registration_lease_ttl_sec = 3;

        // CvmHttpServer（外部 HTTP 接口）配置；http_port == 0 表示不启动。
        std::string http_host = "0.0.0.0";
        uint16_t http_port = 0;

        // 集群中允许同时 serving 的 submaster 上限（名额协调，先到先得）。
        // 排名前 submaster_count 个 master 为 kPrimary，其余降级为 kStandby。
        uint32_t submaster_count = 1;

        // 成员协调（ReconcileRole）的调度周期。slot 归属为本地确定性推导，
        // 不再周期性回写视图快照。
        std::chrono::milliseconds sync_interval{5000};
    };

    explicit CvmController(Config config);
    ~CvmController();

    CvmController(const CvmController&) = delete;
    CvmController& operator=(const CvmController&) = delete;

    void SetDelegate(CvmServiceDelegate* delegate);

    // 当前角色（随名额协调动态变化）。
    MasterRole GetCurrentRole() const { return current_role_.load(); }

    // 排名第一（先到先得）的 primary 的 RPC 地址，作为 standby 的单源回放
    // 目标。当成员列表为空或本机即为排名第一的 primary 时返回空字符串。
    std::string GetPrimaryAddress();

    // standby 动态绑定：按「本 standby 负责的 slot 区间」，用本地一致性哈希
    // 环（与客户端/服务端一致）推导出拥有这些 slot 的 primary，作为回放源
    // （而非回放全部 primary）。本机为 primary 或无法确定负责区间时返回空列表。
    std::vector<MasterRegistration> GetBindingSources();

    ErrorCode Start();
    void Stop();

    // etcd lease id backing this master's registration. Callers may reuse it
    // for their own records (segment mounts) so they share the same lifecycle
    // and are auto-removed on master death. 0 until Start() succeeds.
    EtcdLeaseId GetLeaseId() const { return lease_id_; }

   private:
    struct WatchState {
        std::mutex mutex;
        std::condition_variable cv;
        bool dirty = false;
        bool broken = false;
    };

    Config config_;
    CvmServiceDelegate* delegate_ = nullptr;

    EtcdLeaseId lease_id_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> masters_watch_armed_{false};

    // 当前角色（随名额协调动态变化）；初始为启动配置的 role。
    std::atomic<MasterRole> current_role_{MasterRole::kPrimary};

    // 上次观测到的成员集（master_id 排序去重后），MastersWatchLoop 据此探测
    // 成员增删并打 membership change 日志（joined/left + 当前 primaries）。
    // 仅 MastersWatchLoop 线程访问，无需加锁。
    std::vector<std::string> last_member_ids_;

    std::unique_ptr<WatchState> masters_watch_state_;
    std::thread masters_watch_thread_;
    std::thread keepalive_thread_;
    std::thread membership_thread_;

    std::mutex membership_mutex_;
    std::condition_variable membership_cv_;

    std::unique_ptr<CvmHttpServer> http_server_;

    void CancelMastersWatchAndWait();
    void MastersWatchLoop();
    void KeepaliveLoop();
    void MembershipLoop();
    // 重算并回写角色（先到先得排名）；membership 轮询与 masters watch 回调
    // 共用，用 CAS 去重，保证并发下仅一次迁移与通知。
    void ReconcileRole();
    MasterRole ComputeDesiredRole();
    // 加载所有存活 master 并按 master_id 稳定排序（tie 一致）。失败返回 false。
    bool LoadRankedMembers(std::vector<MasterRegistration>& out);
    // 对比成员集相对上次是否变化，变化时记录一条 membership change 日志
    // （joined/left/当前 primaries）。仅 MastersWatchLoop 调用。
    void LogMembershipChange(const std::vector<MasterRegistration>& members);

    static void WatchCallback(void* ctx, const char* key, size_t key_size,
                              const char* value, size_t value_size,
                              int event_type, int64_t mod_revision);
};

}  // namespace cvm
}  // namespace mooncake