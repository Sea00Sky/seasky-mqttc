#pragma once

// 多端点故障转移：在传输层做一个装饰器，核心状态机（Client）一行都不用改。
//
// 思路：Client::begin_connect() 每次都是"close() 再 connect()"。装饰器把一次
// connect() 变成"按优先顺序依次试所有端点"，撞上死掉的端点就立刻换下一个，
// 不用等退避定时器。连上哪个，下次就先试哪个——否则每次都要先撞一遍死的那个。
//
// 每个端点一次 connect 的超时由工厂造出来的传输层自己决定（例如
// TcpTransportOptions::connect_timeout_ms）。所以 N 个端点的一次 connect()
// 最坏耗时是 N × 单端点预算；端点表长的时候要把单端点预算调小。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mqttc/result.hpp"
#include "mqttc/transport.hpp"

namespace mqttc {

struct Endpoint {
    std::string host;
    uint16_t port = 1883;
};

// "host:port"；端口缺省时用 DefaultPort
std::string to_string(const Endpoint& endpoint);

// 解析 "host[:port][,host[:port]]…"：逗号分隔，端口省了就用默认值。
// host 为空、端口不是 1..65535、整串为空都返回 InvalidArgument。
Result<std::vector<Endpoint>> parse_endpoints(std::string_view text, uint16_t default_port = 1883);

// 造一条到某个端点的传输层。返回 nullptr 表示这个端点连造都造不出来（按失败算）。
using TransportFactory = std::function<std::unique_ptr<Transport>(const Endpoint&)>;

class FailoverTransport : public Transport {
public:
    FailoverTransport(std::vector<Endpoint> endpoints, TransportFactory factory);

    FailoverTransport(const FailoverTransport&) = delete;
    FailoverTransport& operator=(const FailoverTransport&) = delete;

    // 依次尝试所有端点，全部失败返回最后一次的错误
    Error connect() override;
    Result<size_t> read(uint8_t* buffer, size_t capacity) override;
    Error write(const uint8_t* data, size_t size) override;
    // 只关当前这条；不改"优先端点"，重连时还是先从上次连上的那个试
    void close() override;
    // 最后一次失败的端点与原因（例如 TLS 证书错误），全部成功时是空串
    std::string last_error() const override { return last_error_; }

    // 发生过几次"撞死端点后换到别的端点"（装饰器自己的计数，不污染 Client::Stats）
    size_t switches() const { return switches_; }
    size_t endpoint_attempts() const { return endpoint_attempts_; }
    size_t endpoint_count() const { return endpoints_.size(); }
    // 当前优先端点（还没连过时是第 0 个）
    const Endpoint& preferred_endpoint() const { return endpoints_[preferred_]; }
    bool connected() const { return current_ != nullptr; }

private:
    std::vector<Endpoint> endpoints_;
    TransportFactory factory_;
    std::unique_ptr<Transport> current_;
    size_t preferred_ = 0;
    size_t switches_ = 0;
    size_t endpoint_attempts_ = 0;
    std::string last_error_;
};

} // namespace mqttc
