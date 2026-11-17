// 多端点故障转移装饰器的实现，以及端点串的解析。

#include "mqttc/failover.hpp"

#include <cstdlib>
#include <utility>

namespace mqttc {
namespace {

std::string_view trim(std::string_view text) {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) text.remove_prefix(1);
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) text.remove_suffix(1);
    return text;
}

bool parse_port(std::string_view text, uint16_t& out) {
    if (text.empty() || text.size() > 5) return false;
    uint32_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') return false;
        value = value * 10 + static_cast<uint32_t>(c - '0');
    }
    if (value == 0 || value > 65535) return false;
    out = static_cast<uint16_t>(value);
    return true;
}

} // namespace

std::string to_string(const Endpoint& endpoint) {
    return endpoint.host + ":" + std::to_string(endpoint.port);
}

Result<std::vector<Endpoint>> parse_endpoints(std::string_view text, uint16_t default_port) {
    std::vector<Endpoint> endpoints;
    if (trim(text).empty()) return Result<std::vector<Endpoint>>::failure(Error::InvalidArgument);

    size_t start = 0;
    while (start <= text.size()) {
        size_t comma = text.find(',', start);
        if (comma == std::string_view::npos) comma = text.size();

        const std::string_view token = trim(text.substr(start, comma - start));
        if (token.empty()) return Result<std::vector<Endpoint>>::failure(Error::InvalidArgument);

        Endpoint endpoint;
        endpoint.port = default_port;
        const size_t colon = token.find(':');
        if (colon == std::string_view::npos) {
            endpoint.host = std::string(token);
        } else {
            endpoint.host = std::string(trim(token.substr(0, colon)));
            const std::string_view port_text = trim(token.substr(colon + 1));
            if (!parse_port(port_text, endpoint.port)) {
                return Result<std::vector<Endpoint>>::failure(Error::InvalidArgument);
            }
        }
        if (endpoint.host.empty()) {
            return Result<std::vector<Endpoint>>::failure(Error::InvalidArgument);
        }
        endpoints.push_back(std::move(endpoint));

        if (comma == text.size()) break;
        start = comma + 1;
    }

    return Result<std::vector<Endpoint>>::success(std::move(endpoints));
}

FailoverTransport::FailoverTransport(std::vector<Endpoint> endpoints, TransportFactory factory)
    : endpoints_(std::move(endpoints)), factory_(std::move(factory)) {}

Error FailoverTransport::connect() {
    close();
    last_error_.clear();
    if (endpoints_.empty()) return Error::InvalidArgument;

    Error last_error = Error::Closed;
    bool failed_any = false;

    for (size_t i = 0; i < endpoints_.size(); ++i) {
        const size_t index = (preferred_ + i) % endpoints_.size();
        ++endpoint_attempts_;

        std::unique_ptr<Transport> candidate = factory_ ? factory_(endpoints_[index]) : nullptr;
        if (!candidate) {
            last_error = Error::Closed;
            last_error_ = to_string(endpoints_[index]) + ": 无法创建传输层";
            failed_any = true;
            continue;
        }

        const Error error = candidate->connect();
        if (error != Error::None) {
            last_error = error;  // 留着最后一个错误上报（更可能是"最近一次真实的失败原因"）
            const std::string detail = candidate->last_error();
            last_error_ = to_string(endpoints_[index]) + (detail.empty() ? "" : ": " + detail);
            failed_any = true;
            continue;
        }

        current_ = std::move(candidate);
        preferred_ = index;   // 下次先试这次连上的端点
        last_error_.clear();  // 连上了就别再拿上一个端点的错误误导调用方
        if (failed_any) ++switches_;
        return Error::None;
    }

    return last_error;
}

Result<size_t> FailoverTransport::read(uint8_t* buffer, size_t capacity) {
    if (!current_) return Result<size_t>::failure(Error::Closed);
    return current_->read(buffer, capacity);
}

Error FailoverTransport::write(const uint8_t* data, size_t size) {
    if (!current_) return Error::Closed;
    return current_->write(data, size);
}

void FailoverTransport::close() {
    if (current_) {
        current_->close();
        current_.reset();
    }
}

} // namespace mqttc
