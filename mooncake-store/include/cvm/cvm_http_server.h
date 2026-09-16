#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include <ylt/coro_http/coro_http_server.hpp>

#include "types.h"

namespace mooncake {
namespace cvm {

// CVM 三模块之一：外部 HTTP 接口（cvmhttpserver）。
//
// 提供只读 HTTP API，直接聚合 etcd 中的 segment 视图并返回 JSON，供网页 /
// 外部客户端展示。segment 视图由「中立描述符 segments/{seg_id}」与「每 master
// 挂载记录 snapshot/{master_id}/segments/{seg_id}」聚合而来（见 cvm_keys.h）。
class CvmHttpServer {
   public:
    struct Config {
        std::string host = "0.0.0.0";
        uint16_t port = 0;
        std::string cluster_namespace;
    };

    explicit CvmHttpServer(Config config);
    ~CvmHttpServer();

    CvmHttpServer(const CvmHttpServer&) = delete;
    CvmHttpServer& operator=(const CvmHttpServer&) = delete;

    ErrorCode Start();
    void Stop();

    // 聚合 snapshot/*/segments/ 的挂载记录与 segments/* 的描述符，返回
    // segment 视图 JSON；读取失败时返回空串（成功时返回含 `segments` 数组的
    // JSON，可能为空数组）。
    std::string GetSegmentViewJson() const;

   private:
    void InitRoutes();

    Config config_;
    std::unique_ptr<coro_http::coro_http_server> server_;

    std::atomic<bool> running_{false};
};

}  // namespace cvm
}  // namespace mooncake