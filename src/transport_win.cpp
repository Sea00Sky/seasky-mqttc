// Windows 传输层：WSAStartup 的 RAII 守卫 + select() 轮询。
// w64devkit 没有 poll.h，所以这里只能用 select()。

#include "mqttc/transport.hpp"

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#include <algorithm>
#include <climits>
#include <utility>

namespace mqttc {
namespace {

constexpr size_t kMaxIoChunk = 1u << 20;

// 进程内只做一次 WSAStartup，退出时 WSACleanup
struct WinsockGuard {
    WinsockGuard() {
        WSADATA data;
        WSAStartup(MAKEWORD(2, 2), &data);
    }
    ~WinsockGuard() { WSACleanup(); }
};

void ensure_winsock() {
    static WinsockGuard guard;
    (void)guard;
}

SOCKET raw(intptr_t handle) { return static_cast<SOCKET>(handle); }

void set_nonblocking(SOCKET handle, bool value) {
    u_long mode = value ? 1 : 0;
    ioctlsocket(handle, FIONBIO, &mode);
}

// 剩余预算（毫秒）：-1 表示无限等待，0 表示预算已用完
int remaining_ms(bool has_deadline, uint64_t deadline) {
    if (!has_deadline) return -1;
    const uint64_t now = monotonic_ms();
    if (now >= deadline) return 0;
    const uint64_t left = deadline - now;
    return left > static_cast<uint64_t>(INT_MAX) ? INT_MAX : static_cast<int>(left);
}

// 非阻塞 connect + select 等可写，避免黑洞地址把调用线程挂住几分钟
bool connect_with_deadline(SOCKET handle, const addrinfo* item, bool has_deadline,
                          uint64_t deadline, bool& timed_out) {
    set_nonblocking(handle, true);

    if (::connect(handle, item->ai_addr, static_cast<int>(item->ai_addrlen)) == 0) {
        set_nonblocking(handle, false);
        return true;
    }

    const int code = WSAGetLastError();
    if (code != WSAEWOULDBLOCK && code != WSAEINPROGRESS) return false;

    for (;;) {
        const int wait_ms = remaining_ms(has_deadline, deadline);
        if (wait_ms == 0) {
            timed_out = true;
            return false;
        }

        fd_set writable;
        fd_set failed;
        FD_ZERO(&writable);
        FD_ZERO(&failed);
        FD_SET(handle, &writable);
        FD_SET(handle, &failed);

        timeval timeout{};
        const timeval* wait = nullptr;
        if (wait_ms >= 0) {
            timeout.tv_sec = wait_ms / 1000;
            timeout.tv_usec = (wait_ms % 1000) * 1000;
            wait = &timeout;
        }

        const int ready = select(0, nullptr, &writable, &failed, wait);
        if (ready == SOCKET_ERROR) return false;
        if (ready == 0) {
            timed_out = true;
            return false;
        }

        int error = 0;
        int length = static_cast<int>(sizeof(error));
        if (getsockopt(handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &length) != 0) {
            return false;
        }
        if (error != 0) return false;  // 这个地址连不上，让调用方试下一个

        set_nonblocking(handle, false);
        return true;
    }
}

void apply_socket_options(SOCKET handle, uint32_t send_timeout_ms) {
    const int nodelay = 1;
    setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay),
               static_cast<int>(sizeof(nodelay)));

    const int keepalive = 1;
    setsockopt(handle, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<const char*>(&keepalive),
               static_cast<int>(sizeof(keepalive)));

    if (send_timeout_ms > 0) {
        const DWORD timeout = send_timeout_ms;
        setsockopt(handle, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout),
                   static_cast<int>(sizeof(timeout)));
    }
}

} // namespace

TcpTransport::TcpTransport(std::string host, uint16_t port, TcpTransportOptions options)
    : host_(std::move(host)), port_(port), options_(options) {
    ensure_winsock();
}

TcpTransport::~TcpTransport() { close(); }

Error TcpTransport::connect() {
    ensure_winsock();
    close();

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    const std::string service = std::to_string(port_);
    addrinfo* results = nullptr;
    if (getaddrinfo(host_.c_str(), service.c_str(), &hints, &results) != 0) return Error::Closed;

    const bool has_deadline = options_.connect_timeout_ms > 0;
    const uint64_t deadline = monotonic_ms() + options_.connect_timeout_ms;
    bool timed_out = false;

    for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
        const SOCKET handle = socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (handle == INVALID_SOCKET) continue;
        if (connect_with_deadline(handle, item, has_deadline, deadline, timed_out)) {
            apply_socket_options(handle, options_.send_timeout_ms);
            handle_ = static_cast<intptr_t>(handle);
            break;
        }
        closesocket(handle);
        if (timed_out) break;  // 预算用完，再试也没时间了
    }
    freeaddrinfo(results);

    if (handle_ != -1) return Error::None;
    return timed_out ? Error::Timeout : Error::Closed;
}

Result<size_t> TcpTransport::read(uint8_t* buffer, size_t capacity) {
    if (handle_ == -1) return Result<size_t>::failure(Error::Closed);
    if (capacity == 0) return Result<size_t>::failure(Error::WouldBlock);

    fd_set readable;
    FD_ZERO(&readable);
    FD_SET(raw(handle_), &readable);

    timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;

    const int ready = select(0, &readable, nullptr, nullptr, &timeout);
    if (ready == SOCKET_ERROR) return Result<size_t>::failure(Error::Closed);
    if (ready == 0) return Result<size_t>::failure(Error::WouldBlock);

    const size_t chunk = std::min(capacity, kMaxIoChunk);
    const int received = recv(raw(handle_), reinterpret_cast<char*>(buffer), static_cast<int>(chunk), 0);
    if (received == 0) return Result<size_t>::failure(Error::Closed);  // 对端关闭
    if (received == SOCKET_ERROR) {
        if (WSAGetLastError() == WSAEWOULDBLOCK) return Result<size_t>::failure(Error::WouldBlock);
        return Result<size_t>::failure(Error::Closed);
    }
    return Result<size_t>::success(static_cast<size_t>(received));
}

Error TcpTransport::write(const uint8_t* data, size_t size) {
    if (handle_ == -1) return Error::Closed;

    size_t sent_total = 0;
    while (sent_total < size) {
        const size_t chunk = std::min(size - sent_total, kMaxIoChunk);
        const int sent = send(raw(handle_), reinterpret_cast<const char*>(data + sent_total),
                              static_cast<int>(chunk), 0);
        if (sent > 0) {
            sent_total += static_cast<size_t>(sent);
            continue;
        }
        if (sent == SOCKET_ERROR) {
            const int code = WSAGetLastError();
            // SO_SNDTIMEO 到了：链路还在但堵着，这是"临时的"，不是"对端关了"
            if (code == WSAEWOULDBLOCK || code == WSAETIMEDOUT) return Error::Timeout;
            if (code == WSAEINTR) continue;
        }
        return Error::Closed;
    }
    return Error::None;
}

void TcpTransport::close() {
    if (handle_ == -1) return;
    closesocket(raw(handle_));
    handle_ = -1;
}

} // namespace mqttc

#endif // _WIN32
