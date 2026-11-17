#pragma once

// 最小 Result<T>：编解码与 IO 不走异常，失败一律返回错误码。
// 约二十行，不引第三方。

#include <optional>
#include <utility>

namespace mqttc {

enum class Error {
    None,           // 无错误：给返回 Error 的接口当成功值用
    WouldBlock,     // 数据不足 / 资源暂时不够，等会儿再试
    Closed,         // 对端关闭
    Timeout,
    Oversize,       // 超过协议长度上限
    ProtocolError,  // 报文非法
    Busy,           // 到达自己的资源上限（在途消息数、队列长度…），调用方该退让
    InvalidArgument, // 调用方传进来的参数不成立（空端点表、解析不了的 host:port…）
    Tls,            // TLS 握手失败的其它情况（配置、密钥、协议不匹配…）
    CertVerify,     // 对端证书校验失败（CA 不认、主机名不匹配、过期…）：重试没有意义
};

inline const char* error_name(Error error) {
    switch (error) {
    case Error::None:
        return "None";
    case Error::WouldBlock:
        return "WouldBlock";
    case Error::Closed:
        return "Closed";
    case Error::Timeout:
        return "Timeout";
    case Error::Oversize:
        return "Oversize";
    case Error::ProtocolError:
        return "ProtocolError";
    case Error::Busy:
        return "Busy";
    case Error::InvalidArgument:
        return "InvalidArgument";
    case Error::Tls:
        return "Tls";
    case Error::CertVerify:
        return "CertVerify";
    }
    return "Unknown";
}

template <typename T>
class Result {
public:
    static Result success(T value) {
        Result result;
        result.value_.emplace(std::move(value));
        return result;
    }

    static Result failure(Error error) {
        Result result;
        result.error_ = error;
        return result;
    }

    bool ok() const { return value_.has_value(); }
    explicit operator bool() const { return ok(); }

    T& value() { return *value_; }
    const T& value() const { return *value_; }
    T take() { return std::move(*value_); }
    Error error() const { return error_; }

private:
    Result() = default;

    std::optional<T> value_;
    Error error_ = Error::ProtocolError;
};

} // namespace mqttc
