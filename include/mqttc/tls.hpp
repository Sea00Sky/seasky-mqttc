#pragma once

// TLS 传输层（OpenSSL）。TLS 只是 Transport 的另一种实现，Client 与状态机
// 完全不知道它的存在——这样同步路径、异步外壳、故障转移都不用为 TLS 改一行。
//
// 当前实现只在 POSIX 上（Linux/macOS 的 OpenSSL 路径）。Windows 侧需要 MSYS2 +
// OpenSSL 的环境来编写与验证，尚未提供；CMake 在 WIN32 + MQTTC_ENABLE_TLS=ON
// 时会直接报错说明原因。这个头文件本身不包含 OpenSSL 头，所以关掉 TLS 的构建
// 不会看到任何 OpenSSL 类型。

#include <cstdint>
#include <memory>
#include <string>

#include "mqttc/transport.hpp"

namespace mqttc {

struct TlsOptions {
    // 信任材料：CA 文件与/或 CA 目录。两者都空时用 OpenSSL 的系统默认路径
    // （Linux 上大致是 /etc/ssl/certs）。
    std::string ca_file;
    std::string ca_path;

    // 客户端证书与私钥（mTLS）。只在服务端要求客户端证书时填；只给其中一个算配置错误。
    std::string cert_file;
    std::string key_file;

    // SNI 与主机名校验用的名字。空则用 connect 的 host。
    // IP 字面量不发 SNI，校验走 X509_VERIFY_PARAM_set1_ip_asc（证书里要有 IP SAN）。
    std::string server_name;

    // 默认必须校验对端证书。只有调用方显式关掉（CLI 的 --insecure）才会
    // SSL_VERIFY_NONE——那样连接就没有身份认证，中间人可以随便冒充 broker。
    bool verify_peer = true;

    // 最低协议版本："1.2"（默认）或 "1.3"。其它值在 connect() 时报 Error::Tls。
    std::string min_tls_version = "1.2";

    // 握手阶段的总预算（含 TCP 连接之后的 TLS 协商）
    uint32_t handshake_timeout_ms = 10000;
};

// connect() 的错误映射：
// - TCP 连不上 → Closed，预算用完 → Timeout（与 TcpTransport 一致）
// - 证书链/主机名/有效期校验失败 → CertVerify（Client 默认把它当永久失败）
// - 其它握手/配置错误 → Tls，具体原因见 last_error()
// read()/write() 遇到致命的 TLS 错误按 Closed 返回（链路已不可用），
// 具体原因同样放在 last_error() 里。
class TlsTransport : public Transport {
public:
    TlsTransport(std::string host, uint16_t port, TlsOptions tls_options = TlsOptions(),
                 TcpTransportOptions tcp_options = TcpTransportOptions());
    ~TlsTransport() override;

    TlsTransport(const TlsTransport&) = delete;
    TlsTransport& operator=(const TlsTransport&) = delete;

    Error connect() override;
    Result<size_t> read(uint8_t* buffer, size_t capacity) override;
    Error write(const uint8_t* data, size_t size) override;
    void close() override;

    std::string last_error() const override;

private:
    struct State;  // OpenSSL 类型只在 src/tls_transport.cpp 里出现

    bool ensure_context();
    Error fail(std::string message, Error error);

    std::string host_;
    uint16_t port_;
    TlsOptions tls_options_;
    TcpTransportOptions tcp_options_;
    std::unique_ptr<State> state_;
};

} // namespace mqttc