#pragma once

// POSIX socket 小工具：TcpTransport 与 TlsTransport 共用这份。
// 只在非 Windows 的翻译单元里包含（两个使用者都套在 #ifndef _WIN32 里）。
//
// 这里刻意保持"小函数 + 内联"：没有全局状态，也不隐藏资源所有权——
// fd 由调用方自己关，函数只负责解析、连接、等待。

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <string>

#include "mqttc/transport.hpp"  // monotonic_ms

namespace mqttc {
namespace posix_socket {

constexpr size_t kMaxIoChunk = 1u << 20;

// Linux 有 MSG_NOSIGNAL；macOS/BSD 没有，改用 SO_NOSIGPIPE（见 apply_socket_options）
#if defined(MSG_NOSIGNAL)
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

inline void set_nonblocking(int fd, bool value) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return;
    const int updated = value ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    fcntl(fd, F_SETFL, updated);
}

// 剩余预算（毫秒）：-1 表示无限等待，0 表示预算已用完
inline int remaining_ms(bool has_deadline, uint64_t deadline) {
    if (!has_deadline) return -1;
    const uint64_t now = monotonic_ms();
    if (now >= deadline) return 0;
    const uint64_t left = deadline - now;
    return left > static_cast<uint64_t>(INT_MAX) ? INT_MAX : static_cast<int>(left);
}

inline bool connect_errno_is_pending(int code) { return code == EINPROGRESS || code == EALREADY; }

// 非阻塞 connect + poll 等可写，避免黑洞地址把调用线程挂住几分钟
inline bool connect_with_deadline(int fd, const addrinfo* item, bool has_deadline,
                                  uint64_t deadline, bool& timed_out) {
    set_nonblocking(fd, true);

    if (::connect(fd, item->ai_addr, item->ai_addrlen) == 0) {
        set_nonblocking(fd, false);
        return true;
    }
    if (!connect_errno_is_pending(errno)) return false;

    for (;;) {
        const int wait_ms = remaining_ms(has_deadline, deadline);
        if (wait_ms == 0) {
            timed_out = true;
            return false;
        }

        pollfd item_poll{};
        item_poll.fd = fd;
        item_poll.events = POLLOUT;

        const int ready = ::poll(&item_poll, 1, wait_ms);
        if (ready < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (ready == 0) {
            timed_out = true;
            return false;
        }

        int error = 0;
        socklen_t length = sizeof(error);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) != 0) return false;
        if (error != 0) return false;  // 这个地址连不上，让调用方试下一个

        set_nonblocking(fd, false);
        return true;
    }
}

inline void apply_socket_options(int fd, uint32_t send_timeout_ms) {
    const int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    const int keepalive = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));

#if !defined(MSG_NOSIGNAL) && defined(SO_NOSIGPIPE)
    const int nosigpipe = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
#endif

    if (send_timeout_ms > 0) {
        timeval timeout{};
        timeout.tv_sec = static_cast<time_t>(send_timeout_ms / 1000);
        timeout.tv_usec = static_cast<suseconds_t>((send_timeout_ms % 1000) * 1000);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    }
}

// 解析 host:port 并逐个地址尝试连接。成功返回 fd（阻塞模式），失败返回 -1。
// timed_out 表示总预算用完（区别于"所有地址都连不上"）。
inline int connect_tcp(const std::string& host, uint16_t port, uint32_t connect_timeout_ms,
                       bool& timed_out) {
    timed_out = false;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    const std::string service = std::to_string(port);
    addrinfo* results = nullptr;
    if (getaddrinfo(host.c_str(), service.c_str(), &hints, &results) != 0) return -1;

    const bool has_deadline = connect_timeout_ms > 0;
    const uint64_t deadline = monotonic_ms() + connect_timeout_ms;
    int connected = -1;

    for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
        const int fd = ::socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (fd < 0) continue;
        if (connect_with_deadline(fd, item, has_deadline, deadline, timed_out)) {
            connected = fd;
            break;
        }
        ::close(fd);
        if (timed_out) break;  // 预算用完，再试也没时间了
    }
    freeaddrinfo(results);
    return connected;
}

// poll 一次；返回 <0 表示出错（EINTR 已重试），0 表示超时，>0 表示就绪。
// TLS 的握手/写入循环需要区分"超时"与"poll 出错"，所以单独暴露这个。
inline int poll_fd(int fd, short events, int timeout_ms) {
    pollfd item{};
    item.fd = fd;
    item.events = events;
    for (;;) {
        const int ready = ::poll(&item, 1, timeout_ms);
        if (ready < 0 && errno == EINTR) continue;
        return ready;
    }
}

// 等 fd 可读/可写（阻塞到 timeout_ms；-1 = 一直等）。
// 返回 false 表示超时或 poll 出错——调用方自己按截止时间判断是哪一种。
inline bool wait_fd(int fd, short events, int timeout_ms) {
    return poll_fd(fd, events, timeout_ms) > 0;
}

} // namespace posix_socket
} // namespace mqttc