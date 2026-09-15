#include <fstream>
#include <iostream>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "etcd_helper.h"
#include "vsegment/partition_quota_planner.h"

DEFINE_string(input, "", "Path to a JSON PartitionQuotaPlanRequest");
DEFINE_string(etcd_endpoints, "", "Semicolon-separated ETCD endpoints");
DEFINE_string(cluster_namespace, "", "CVM cluster namespace");
DEFINE_uint64(max_etcd_value_bytes, 1500000,
              "Maximum serialized ETCD value size");
DEFINE_bool(dry_run, false, "Print the candidate snapshot without publishing");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    if (FLAGS_input.empty()) {
        LOG(ERROR) << "--input is required";
        return 2;
    }
    std::ifstream input(FLAGS_input, std::ios::binary);
    if (!input) {
        LOG(ERROR) << "cannot open " << FLAGS_input;
        return 2;
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    mooncake::vsegment::PartitionQuotaPlanRequest request;
    try {
        struct_json::from_json(request, contents.str());
    } catch (const std::exception& error) {
        LOG(ERROR) << "invalid planner input: " << error.what();
        return 2;
    }
    auto result = mooncake::vsegment::PartitionQuotaPlanner().Plan(request);
    if (!result) {
        LOG(ERROR) << result.detail;
        return 3;
    }
    std::string serialized;
    struct_json::to_json(result.snapshot, serialized);
    if (FLAGS_dry_run) {
        std::cout << serialized << std::endl;
        return 0;
    }
    if (FLAGS_etcd_endpoints.empty()) {
        LOG(ERROR) << "--etcd_endpoints is required unless --dry_run is set";
        return 2;
    }
    if (FLAGS_cluster_namespace.empty()) {
        LOG(ERROR) << "--cluster_namespace is required unless --dry_run is set";
        return 2;
    }
    auto error = mooncake::EtcdHelper::ConnectToEtcdStoreClient(
        FLAGS_etcd_endpoints);
    if (error != mooncake::ErrorCode::OK) return 4;
    mooncake::vsegment::EtcdPartitionQuotaSnapshotStore store(
        FLAGS_cluster_namespace);
    std::string detail;
    error = store.Create(result.snapshot, FLAGS_max_etcd_value_bytes, &detail);
    if (error != mooncake::ErrorCode::OK) {
        LOG(ERROR) << detail;
        return 5;
    }
    LOG(INFO) << "published vsegment quota generation "
              << result.snapshot.config_generation << " with "
              << result.snapshot.quotas.size() << " partition/profile quotas";
    return 0;
}
