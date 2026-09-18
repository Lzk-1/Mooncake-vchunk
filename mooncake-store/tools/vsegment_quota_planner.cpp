// vsegment_quota_planner: vsegment 静态配额规划工具。
//
// 正常部署方式（自动发现，用户只需 3 个核心策略参数）：
//   vsegment_quota_planner \
//       --etcd_endpoints 127.0.0.1:2379 \
//       --cluster_namespace my-cluster \
//       --member_count 4 --stripe_size 65536 --member_extent_size 1048576 \
//       [--publish]
//
//   系统从 CVM 自动获取已注册 psegment（含 medium/io_alignment/
//   supports_unaligned_io/failure_domain/used_bytes 等资源事实），
//   从 KV PT 自动获取 Partition 列表，校验容量/介质/对齐能力和可分配范围
//   后，计算各 Partition 的静态配额并（可选）发布到 ETCD。默认 dry-run，
//   加 --publish 才真正发布。
//
//   空间安全：按 [used_bytes, capacity) 作为 vsegment 可切分范围，避免与
//   其他分配器占用范围重叠。vsegment_exclusive=true 时校验 used_bytes==0
//   （快路径）；false 时按 [used_bytes, capacity) 切分。用户无需手动声明
//   exclusive。
//
// 离线规划/单元测试（手写完整 JSON，不作为正式部署方式）：
//   vsegment_quota_planner --input request.json [--dry_run]
//   --input 模式保留给离线规划和单元测试，正常部署不应使用。

#include <fstream>
#include <iostream>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "etcd_helper.h"
#include "cvm/etcd_view_store.h"
#include "vsegment/partition_quota_planner.h"

// 模式选择：默认自动发现；--input 切到离线规划（仅测试）。
DEFINE_string(input, "",
              "Path to a JSON PartitionQuotaPlanRequest. OFFLINE PLANNING / "
              "UNIT TEST ONLY — do not use as a regular deployment path. "
              "Leave empty to use automatic CVM discovery (default).");
DEFINE_string(etcd_endpoints, "", "Semicolon-separated ETCD endpoints");
DEFINE_string(cluster_namespace, "", "CVM cluster namespace");
DEFINE_uint64(max_etcd_value_bytes, 1500000,
              "Maximum serialized ETCD value size");

// 用户策略（正常部署只需这 3 个核心参数）。
DEFINE_uint32(member_count, 0, "Number of distinct members per vsegment");
DEFINE_uint64(stripe_size, 0, "Stripe size in bytes");
DEFINE_uint64(member_extent_size, 0, "Extent size per member in bytes");

// 可选策略（提供默认值，不应强制用户填写）。
DEFINE_double(reserved_ratio, 0.0,
              "Fraction reserved outside vsegment quotas, in [0, 1). "
              "Default 0 means no reservation.");
DEFINE_uint32(initial_vsegment_count, 1,
              "Initial vsegment count per partition. Default 1.");
DEFINE_string(default_profile, "default",
              "Default profile name. Default \"default\".");
DEFINE_string(profile_name, "default",
              "Profile name for the single-profile plan. Default \"default\".");
DEFINE_string(required_medium, "REGISTERED_MEMORY",
              "Required medium for the profile. Must match the medium "
              "reported by SegmentDescriptor. Default "
              "\"REGISTERED_MEMORY\".");
DEFINE_uint64(io_alignment, 1,
              "I/O alignment in bytes for the profile. Planner takes "
              "max(profile.io_alignment, segment.io_alignment). Default 1.");
DEFINE_uint64(config_generation, 1,
              "Config generation monotonic number. Default 1.");

// 发布与高级开关。
DEFINE_bool(dry_run, true,
            "Print the candidate snapshot without publishing. Default true; "
            "set to false with --publish to publish.");
DEFINE_bool(publish, false,
            "Publish the snapshot to ETCD (requires --dry_run=false). "
            "Default false (safe).");
DEFINE_bool(allow_non_exclusive, false,
            "[DEPRECATED] Kept for backward compatibility. Automatic "
            "discovery now uses [used_bytes, capacity) as the allocatable "
            "range by default, so non-exclusive segments are accepted "
            "without this flag. No longer has any effect.");

namespace {

int RunFromInput(mooncake::vsegment::PartitionQuotaPlanRequest& request) {
    if (FLAGS_input.empty()) {
        LOG(ERROR) << "--input is required for offline planning mode";
        return 2;
    }
    std::ifstream input(FLAGS_input, std::ios::binary);
    if (!input) {
        LOG(ERROR) << "cannot open " << FLAGS_input;
        return 2;
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    try {
        struct_json::from_json(request, contents.str());
    } catch (const std::exception& error) {
        LOG(ERROR) << "invalid planner input: " << error.what();
        return 2;
    }
    LOG(WARNING) << "Offline planning mode (--input): this is preserved for "
                    "unit tests / offline planning and is NOT the regular "
                    "deployment path. Use automatic CVM discovery instead.";
    return 0;
}

int RunFromDiscovery(mooncake::vsegment::PartitionQuotaPlanRequest& request) {
    if (FLAGS_member_count == 0 || FLAGS_stripe_size == 0 ||
        FLAGS_member_extent_size == 0) {
        LOG(ERROR) << "automatic discovery requires --member_count, "
                      "--stripe_size and --member_extent_size";
        return 2;
    }
    if (FLAGS_etcd_endpoints.empty() || FLAGS_cluster_namespace.empty()) {
        LOG(ERROR) << "automatic discovery requires --etcd_endpoints and "
                      "--cluster_namespace";
        return 2;
    }
    auto error = mooncake::EtcdHelper::ConnectToEtcdStoreClient(
        FLAGS_etcd_endpoints);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "cannot connect to etcd: "
                   << mooncake::toString(error);
        return 4;
    }
    std::vector<mooncake::cvm::SegmentDescriptor> descriptors;
    std::vector<mooncake::cvm::MasterRegistration> masters;
    std::vector<std::pair<std::string, mooncake::cvm::MountEntry>> mounts;
    mooncake::ViewVersionId revision = 0;
    error = mooncake::cvm::EtcdViewStore::LoadAllMasters(
        FLAGS_cluster_namespace, masters, revision);
    if (error == mooncake::ErrorCode::OK)
        error = mooncake::cvm::EtcdViewStore::LoadAllSegmentDescriptors(
            FLAGS_cluster_namespace, descriptors, revision);
    if (error == mooncake::ErrorCode::OK)
        error = mooncake::cvm::EtcdViewStore::LoadAllMountEntries(
            FLAGS_cluster_namespace, mounts, revision);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "CVM discovery failed: " << mooncake::toString(error);
        return 4;
    }
    mooncake::vsegment::VSegmentUserPolicy policy;
    policy.member_count = FLAGS_member_count;
    policy.stripe_size = FLAGS_stripe_size;
    policy.member_extent_size = FLAGS_member_extent_size;
    policy.default_profile = FLAGS_default_profile;
    policy.profile_name = FLAGS_profile_name;
    policy.required_medium = FLAGS_required_medium;
    policy.io_alignment = FLAGS_io_alignment;
    policy.initial_vsegment_count = FLAGS_initial_vsegment_count;
    policy.reserved_ratio = FLAGS_reserved_ratio;
    policy.config_generation = FLAGS_config_generation;
    std::string detail;
    error = mooncake::vsegment::BuildDiscoveredQuotaPlan(
        policy, descriptors, masters, mounts, &request, &detail,
        /*allow_non_exclusive=*/FLAGS_allow_non_exclusive);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << detail;
        return 3;
    }
    LOG(INFO) << "Discovered " << request.segments.size()
              << " psegments with usable free space from CVM; "
              << request.partition_ids.size() << " partitions";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    const bool input_mode = !FLAGS_input.empty();
    mooncake::vsegment::PartitionQuotaPlanRequest request;
    int rc = input_mode ? RunFromInput(request) : RunFromDiscovery(request);
    if (rc != 0) return rc;

    auto result = mooncake::vsegment::PartitionQuotaPlanner().Plan(request);
    if (!result) {
        LOG(ERROR) << result.detail;
        return 3;
    }
    std::string serialized;
    struct_json::to_json(result.snapshot, serialized);

    const bool want_publish = FLAGS_publish || !FLAGS_dry_run;
    if (!want_publish) {
        std::cout << serialized << std::endl;
        LOG(WARNING) << "Dry-run only. Re-run with --publish (and "
                        "--dry_run=false) to publish to ETCD.";
        return 0;
    }
    if (FLAGS_etcd_endpoints.empty() || FLAGS_cluster_namespace.empty()) {
        LOG(ERROR) << "--publish requires --etcd_endpoints and "
                      "--cluster_namespace";
        return 2;
    }
    if (!input_mode) {
        // 自动发现模式已连接 etcd；离线模式需要在此连接。
    } else {
        auto error = mooncake::EtcdHelper::ConnectToEtcdStoreClient(
            FLAGS_etcd_endpoints);
        if (error != mooncake::ErrorCode::OK) {
            LOG(ERROR) << "cannot connect to etcd: "
                       << mooncake::toString(error);
            return 4;
        }
    }
    mooncake::vsegment::EtcdPartitionQuotaSnapshotStore store(
        FLAGS_cluster_namespace);
    std::string detail;
    auto error = store.Create(result.snapshot, FLAGS_max_etcd_value_bytes,
                              &detail);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << detail;
        return 5;
    }
    LOG(INFO) << "published vsegment quota generation "
              << result.snapshot.config_generation << " with "
              << result.snapshot.quotas.size() << " partition/profile quotas";
    return 0;
}
