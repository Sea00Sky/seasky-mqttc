// OpenSSL 传输层（POSIX）。分两层：
//   1) TcpTransport / posix_socket 负责 TCP 连接本身；
//   2) 这里只做 TLS：SSL_CTX 配置、SNI、主机名校验、握手、读写与关闭。
//
// 契约的要点：
// - socket 全程保持非阻塞。SSL_read/SSL_write 用 WANT_READ/WANT_WRITE 驱动，
//   我们自己 poll；read() 永远不阻塞（等不到数据就是 WouldBlock）。
// - write() 必须内部循环直到写完或有界超时，保住 Transport 的"all-or-error"契约，
//   这样 Client::send_packet 与状态机一行都不用改。
// - 证书校验失败映射成 Error::CertVerify（Client 默认判永久失败），其它 TLS 错误
//   是 Error::Tls；具体原因（verify 结果、OpenSSL 错误栈）放 last_error()。

#include "mqttc/tls.hpp"

#ifndef _WIN32

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "socket_posix.hpp"

namespace mqttc {
namespace {

namespace ps = posix_socket;

std::string openssl_error_text() {
    std::string text;
    unsigned long code = 0;
    while ((code = ERR_get_error()) != 0) {
        char buffer[256];
        ERR_error_string_n(code, buffer, sizeof(buffer));
        if (!text.empty()) text += "; ";
        text += buffer;
    }
    return text;
}

// IP 字面量不做 SNI；校验时用 set1_ip_asc（证书里要带 IP SAN）
bool is_ip_literal(const std::string& text) {
    unsigned char storage[sizeof(in6_addr)];
    return inet_pton(AF_INET, text.c_str(), storage) == 1 ||
           inet_pton(AF_INET6, text.c_str(), storage) == 1;
}

} // namespace

struct TlsTransport::State {
    SSL_CTX* context = nullptr;  // 跨重连复用；析构时释放
    SSL* ssl = nullptr;
    int fd = -1;
    std::string last_error;
};

TlsTransport::TlsTransport(std::string host, uint16_t port, TlsOptions tls_options,
                           TcpTransportOptions tcp_options)
    : host_(std::move(host)), port_(port), tls_options_(std::move(tls_options)),
      tcp_options_(tcp_options), state_(std::make_unique<State>()) {}

TlsTransport::~TlsTransport() {
    close();
    if (state_->context != nullptr) SSL_CTX_free(state_->context);
}

std::string TlsTransport::last_error() const {
    return state_ ? state_->last_error : std::string();
}

Error TlsTransport::fail(std::string message, Error error) {
    state_->last_error = std::move(message);
    return error;
}

bool TlsTransport::ensure_context() {
    if (state_->context != nullptr) return true;

    ERR_clear_error();
    SSL_CTX* context = SSL_CTX_new(TLS_client_method());
    if (context == nullptr) {
        state_->last_error = "SSL_CTX_new 失败: " + openssl_error_text();
        return false;
    }

    auto abandon = [&](const std::string& message) {
        state_->last_error = message;
        SSL_CTX_free(context);
        return false;
    };

    int min_version = 0;
    if (tls_options_.min_tls_version == "1.2") {
        min_version = TLS1_2_VERSION;
    } else if (tls_options_.min_tls_version == "1.3") {
        min_version = TLS1_3_VERSION;
    } else {
        return abandon("最低 TLS 版本只支持 \"1.2\" 或 \"1.3\"，收到: " +
                       tls_options_.min_tls_version);
    }
    if (SSL_CTX_set_min_proto_version(context, min_version) != 1) {
        return abandon("设置最低 TLS 版本失败: " + openssl_error_text());
    }

    if (tls_options_.verify_peer) {
        SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);

        const char* ca_file = tls_options_.ca_file.empty() ? nullptr : tls_options_.ca_file.c_str();
        const char* ca_path = tls_options_.ca_path.empty() ? nullptr : tls_options_.ca_path.c_str();
        const int loaded = (ca_file != nullptr || ca_path != nullptr)
                               ? SSL_CTX_load_verify_locations(context, ca_file, ca_path)
                               : SSL_CTX_set_default_verify_paths(context);
        if (loaded != 1) {
            return abandon("加载 CA 信任材料失败: " + openssl_error_text());
        }
    } else {
        // 调用方显式放弃了身份认证（CLI 的 --insecure）。危险，但必须让人能显式选。
        SSL_CTX_set_verify(context, SSL_VERIFY_NONE, nullptr);
    }

    if (!tls_options_.cert_file.empty()) {
        if (tls_options_.key_file.empty()) {
            return abandon("给了客户端证书（cert_file）但没给私钥（key_file）");
        }
        if (SSL_CTX_use_certificate_chain_file(context, tls_options_.cert_file.c_str()) != 1) {
            return abandon("加载客户端证书失败: " + openssl_error_text());
        }
        if (SSL_CTX_use_PrivateKey_file(context, tls_options_.key_file.c_str(), SSL_FILETYPE_PEM) !=
            1) {
            return abandon("加载客户端私钥失败: " + openssl_error_text());
        }
        if (SSL_CTX_check_private_key(context) != 1) {
            return abandon("客户端证书与私钥不匹配: " + openssl_error_text());
        }
    } else if (!tls_options_.key_file.empty()) {
        return abandon("给了客户端私钥（key_file）但没给证书（cert_file）");
    }

    state_->context = context;
    return true;
}

Error TlsTransport::connect() {
    close();
    state_->last_error.clear();
    ERR_clear_error();

    bool tcp_timed_out = false;
    const int fd = ps::connect_tcp(host_, port_, tcp_options_.connect_timeout_ms, tcp_timed_out);
    if (fd < 0) {
        return fail(host_ + ":" + std::to_string(port_) +
                        (tcp_timed_out ? " TCP 连接超时" : " TCP 连接失败"),
                    tcp_timed_out ? Error::Timeout : Error::Closed);
    }
    state_->fd = fd;
    // TLS 的读写要靠 WANT_READ/WANT_WRITE；socket 必须保持非阻塞。
    // TcpTransport 会在 connect 后切回阻塞，这里不能学它。
    ps::set_nonblocking(fd, true);

    if (!ensure_context()) {
        close();
        return Error::Tls;
    }

    state_->ssl = SSL_new(state_->context);
    if (state_->ssl == nullptr) {
        const std::string text = "SSL_new 失败: " + openssl_error_text();
        close();
        return fail(text, Error::Tls);
    }
    if (SSL_set_fd(state_->ssl, fd) != 1) {
        const std::string text = "SSL_set_fd 失败: " + openssl_error_text();
        close();
        return fail(text, Error::Tls);
    }

    const std::string verify_name =
        tls_options_.server_name.empty() ? host_ : tls_options_.server_name;
    const bool ip = is_ip_literal(verify_name);

    if (!ip) {
        // SNI 让虚拟主机知道我们要连哪个名字；verify_peer 时再做主机名校验
        if (SSL_set_tlsext_host_name(state_->ssl, verify_name.c_str()) != 1) {
            const std::string text = "设置 SNI 失败: " + openssl_error_text();
            close();
            return fail(text, Error::Tls);
        }
        if (tls_options_.verify_peer && SSL_set1_host(state_->ssl, verify_name.c_str()) != 1) {
            const std::string text = "设置主机名校验失败: " + openssl_error_text();
            close();
            return fail(text, Error::Tls);
        }
    } else if (tls_options_.verify_peer) {
        if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(state_->ssl), verify_name.c_str()) != 1) {
            const std::string text = "设置 IP 校验失败: " + openssl_error_text();
            close();
            return fail(text, Error::Tls);
        }
    }

    // 允许 SSL_write 短写，并在 WANT_WRITE 后原样重试同一段缓冲
    SSL_set_mode(state_->ssl,
                 SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    const bool has_deadline = tls_options_.handshake_timeout_ms > 0;
    const uint64_t deadline = monotonic_ms() + tls_options_.handshake_timeout_ms;

    for (;;) {
        ERR_clear_error();
        const int result = SSL_connect(state_->ssl);
        if (result == 1) break;

        const int reason = SSL_get_error(state_->ssl, result);
        if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
            const int wait_ms = ps::remaining_ms(has_deadline, deadline);
            if (wait_ms == 0) {
                close();
                return fail("TLS 握手超时（" + std::to_string(tls_options_.handshake_timeout_ms) +
                                " ms）",
                            Error::Timeout);
            }
            const short events = reason == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
            const int ready = ps::poll_fd(fd, events, wait_ms);
            if (ready < 0) {
                const std::string text = std::string("TLS 握手等待失败: ") + std::strerror(errno);
                close();
                return fail(text, Error::Tls);
            }
            if (ready == 0) {
                if (!has_deadline) continue;  // 无限预算，poll 理论上不会超时；继续等
                close();
                return fail("TLS 握手超时（" + std::to_string(tls_options_.handshake_timeout_ms) +
                                " ms）",
                            Error::Timeout);
            }
            continue;
        }

        const long verify_result = SSL_get_verify_result(state_->ssl);
        if (verify_result != X509_V_OK) {
            std::string text = std::string("证书校验失败: ") +
                               X509_verify_cert_error_string(verify_result);
            const std::string stack = openssl_error_text();
            if (!stack.empty()) text += "（" + stack + "）";
            close();
            return fail(text, Error::CertVerify);
        }
        std::string text = "TLS 握手失败: " + openssl_error_text();
        if (text == "TLS 握手失败: ") text += "对端关闭或协议不匹配";
        close();
        return fail(text, Error::Tls);
    }

    // 握手成功；verify_peer 时再确认一次结果（防御性，正常路径下已经在上面保证）
    const long verify_result = SSL_get_verify_result(state_->ssl);
    if (tls_options_.verify_peer && verify_result != X509_V_OK) {
        std::string text =
            std::string("证书校验失败: ") + X509_verify_cert_error_string(verify_result);
        close();
        return fail(text, Error::CertVerify);
    }

    return Error::None;
}

Result<size_t> TlsTransport::read(uint8_t* buffer, size_t capacity) {
    if (state_->ssl == nullptr) return Result<size_t>::failure(Error::Closed);
    if (capacity == 0) return Result<size_t>::failure(Error::WouldBlock);

    const int chunk = static_cast<int>(std::min(capacity, ps::kMaxIoChunk));
    ERR_clear_error();
    const int received = SSL_read(state_->ssl, buffer, chunk);
    if (received > 0) return Result<size_t>::success(static_cast<size_t>(received));

    switch (SSL_get_error(state_->ssl, received)) {
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
        return Result<size_t>::failure(Error::WouldBlock);
    case SSL_ERROR_ZERO_RETURN:
        return Result<size_t>::failure(Error::Closed);
    case SSL_ERROR_SYSCALL:
        if (received == 0) return Result<size_t>::failure(Error::Closed);  // 没有 close_notify 的 EOF
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return Result<size_t>::failure(Error::WouldBlock);
        }
        state_->last_error = std::string("TLS 读取失败: ") + std::strerror(errno);
        return Result<size_t>::failure(Error::Closed);
    default: {
        std::string text = "TLS 读取失败: " + openssl_error_text();
        if (text == "TLS 读取失败: ") text += "对端关闭或报文非法";
        state_->last_error = std::move(text);
        return Result<size_t>::failure(Error::Closed);
    }
    }
}

Error TlsTransport::write(const uint8_t* data, size_t size) {
    if (state_->ssl == nullptr) return Error::Closed;

    const bool has_deadline = tcp_options_.send_timeout_ms > 0;
    const uint64_t deadline = monotonic_ms() + tcp_options_.send_timeout_ms;
    size_t sent_total = 0;

    while (sent_total < size) {
        const int chunk = static_cast<int>(std::min(size - sent_total, ps::kMaxIoChunk));
        ERR_clear_error();
        const int written = SSL_write(state_->ssl, data + sent_total, chunk);
        if (written > 0) {
            sent_total += static_cast<size_t>(written);
            continue;
        }

        const int reason = SSL_get_error(state_->ssl, written);
        if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
            const int wait_ms = ps::remaining_ms(has_deadline, deadline);
            if (wait_ms == 0) return fail("TLS 写入超时", Error::Timeout);
            const short events = reason == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
            const int ready = ps::poll_fd(state_->fd, events, wait_ms);
            if (ready < 0) {
                return fail(std::string("TLS 写入等待失败: ") + std::strerror(errno), Error::Closed);
            }
            if (ready == 0) {
                if (!has_deadline) continue;
                return fail("TLS 写入超时", Error::Timeout);
            }
            continue;
        }
        if (reason == SSL_ERROR_ZERO_RETURN || reason == SSL_ERROR_SYSCALL) {
            return fail("TLS 写入失败: 对端关闭", Error::Closed);
        }
        std::string text = "TLS 写入失败: " + openssl_error_text();
        if (text == "TLS 写入失败: ") text += "对端关闭或协议错误";
        return fail(std::move(text), Error::Closed);
    }
    return Error::None;
}

void TlsTransport::close() {
    if (!state_) return;

    if (state_->ssl != nullptr) {
        // 主动发一次 close_notify 就走，不阻塞等对端的回应
        ERR_clear_error();
        SSL_shutdown(state_->ssl);
        SSL_free(state_->ssl);
        state_->ssl = nullptr;
    }
    if (state_->fd >= 0) {
        ::close(state_->fd);
        state_->fd = -1;
    }
}

} // namespace mqttc

#endif // _WIN32