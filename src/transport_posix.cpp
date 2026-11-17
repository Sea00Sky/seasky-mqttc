// POSIX 传输层：poll() 轮询。Linux 用 poll()，macOS/BSD 也走这一份（send 的
// MSG_NOSIGNAL 在 macOS 上不存在，换成 SO_NOSIGPIPE，见 socket_posix.hpp）。

#include "mqttc/transport.hpp"

#ifndef _WIN32

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <utility>

#include "socket_posix.hpp"

namespace mqttc {

namespace ps = posix_socket;

TcpTransport::TcpTransport(std::string host, uint16_t port, TcpTransportOptions options)
    : host_(std::move(host)), port_(port), options_(options) {}

TcpTransport::~TcpTransport() { close(); }

Error TcpTransport::connect() {
    close();

    bool timed_out = false;
    const int fd = ps::connect_tcp(host_, port_, options_.connect_timeout_ms, timed_out);
    if (fd < 0) return timed_out ? Error::Timeout : Error::Closed;

    ps::apply_socket_options(fd, options_.send_timeout_ms);
    handle_ = fd;
    return Error::None;
}

Result<size_t> TcpTransport::read(uint8_t* buffer, size_t capacity) {
    if (handle_ == -1) return Result<size_t>::failure(Error::Closed);
    if (capacity == 0) return Result<size_t>::failure(Error::WouldBlock);

    const int fd = static_cast<int>(handle_);
    if (!ps::wait_fd(fd, POLLIN, 0)) return Result<size_t>::failure(Error::WouldBlock);

    const size_t chunk = std::min(capacity, ps::kMaxIoChunk);
    const ssize_t received = ::recv(fd, buffer, chunk, 0);
    if (received == 0) return Result<size_t>::failure(Error::Closed);
    if (received < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return Result<size_t>::failure(Error::WouldBlock);
        }
        return Result<size_t>::failure(Error::Closed);
    }
    return Result<size_t>::success(static_cast<size_t>(received));
}

Error TcpTransport::write(const uint8_t* data, size_t size) {
    if (handle_ == -1) return Error::Closed;

    size_t sent_total = 0;
    while (sent_total < size) {
        const size_t chunk = std::min(size - sent_total, ps::kMaxIoChunk);
        const ssize_t sent =
            ::send(static_cast<int>(handle_), data + sent_total, chunk, ps::kSendFlags);
        if (sent > 0) {
            sent_total += static_cast<size_t>(sent);
            continue;
        }
        if (sent < 0) {
            // SO_SNDTIMEO 到了：链路还在但堵着，算"临时"，不算"对端关了"
            if (errno == EAGAIN || errno == EWOULDBLOCK) return Error::Timeout;
            if (errno == EINTR) continue;
        }
        return Error::Closed;
    }
    return Error::None;
}

void TcpTransport::close() {
    if (handle_ == -1) return;
    ::close(static_cast<int>(handle_));
    handle_ = -1;
}

} // namespace mqttc

#endif // _WIN32