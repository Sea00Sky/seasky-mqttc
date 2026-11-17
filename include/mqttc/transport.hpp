#pragma once

// 传输层抽象。客户端只认这个接口，测试用内存实现，真连用 TcpTransport。

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "mqttc/result.hpp"
#include "mqttc/wire.hpp"

namespace mqttc {

// 契约：
// - connect()：建立或重建连接；成功返回 Error::None
// - read()：无数据返回 WouldBlock；对端关闭返回 Closed；否则返回读到的字节数（>0）
// - write()：要么写完 size 个字节，要么返回错误（不做部分写）
// - close()：可重复调用
class Transport {
public:
    virtual ~Transport() = default;

    virtual Error connect() = 0;
    virtual Result<size_t> read(uint8_t* buffer, size_t capacity) = 0;
    virtual Error write(const uint8_t* data, size_t size) = 0;
    virtual void close() = 0;

    // 最近一次失败的可读描述；没有额外上下文时是空串。
    // TLS 的证书/握手错误有比 Error 枚举更多的信息（verify 结果、OpenSSL 错误栈），
    // 放在这里由调用方决定怎么呈现；普通传输层用默认实现就够。
    virtual std::string last_error() const { return std::string(); }
};

// 测试用传输层：读的是脚本喂进来的字节，写出来的字节留在缓冲里。
// 不碰 socket、不碰时间，所以客户端状态机可以逐字节、确定性地验证。
class InMemoryTransport : public Transport {
public:
    Error connect() override {
        ++connect_calls_;
        if (fail_connect_) return Error::Closed;
        incoming_.clear();
        connected_ = true;
        closed_ = false;
        return Error::None;
    }

    Result<size_t> read(uint8_t* buffer, size_t capacity) override {
        if (!connected_) return Result<size_t>::failure(Error::Closed);
        if (incoming_.empty()) {
            // 缓冲排空了才报关闭。真 socket 上"对端先发一包再关"也是先读到数据、
            // 下一次才读到 EOF——测试里必须保持同样的顺序，否则测不出这类丢包。
            return Result<size_t>::failure(closed_ ? Error::Closed : Error::WouldBlock);
        }

        const size_t count = std::min({capacity, incoming_.size(), read_chunk_});
        std::copy_n(incoming_.begin(), static_cast<std::ptrdiff_t>(count), buffer);
        incoming_.erase(incoming_.begin(), incoming_.begin() + static_cast<std::ptrdiff_t>(count));
        return Result<size_t>::success(count);
    }

    Error write(const uint8_t* data, size_t size) override {
        if (!connected_ || closed_) return Error::Closed;
        outgoing_.insert(outgoing_.end(), data, data + size);
        return Error::None;
    }

    void close() override {
        connected_ = false;
        incoming_.clear();
    }

    // ---- 测试控制 ----

    void feed(const uint8_t* data, size_t size) {
        incoming_.insert(incoming_.end(), data, data + size);
    }
    void feed(const ByteVec& bytes) { feed(bytes.data(), bytes.size()); }

    void set_closed(bool value) { closed_ = value; }
    void set_connect_fails(bool value) { fail_connect_ = value; }
    void set_read_chunk(size_t bytes) { read_chunk_ = bytes == 0 ? 1 : bytes; }

    const ByteVec& written() const { return outgoing_; }
    void clear_written() { outgoing_.clear(); }
    size_t connect_calls() const { return connect_calls_; }
    bool connected() const { return connected_; }

private:
    ByteVec incoming_;
    ByteVec outgoing_;
    size_t read_chunk_ = 4096;
    size_t connect_calls_ = 0;
    bool connected_ = false;
    bool closed_ = false;
    bool fail_connect_ = false;
};

// 裸 TCP 的超时预算。默认值都是有界的：没有超时的 connect/send 会把调用线程
// 挂在黑洞地址或满窗口的对端上，对后面要跑的 IO 线程是致命的。
// 0 表示不限（不推荐，只留给"我知道自己在干什么"的场合）。
struct TcpTransportOptions {
    uint32_t connect_timeout_ms = 10000;  // 单次 connect() 的总预算，含逐个地址尝试
    uint32_t send_timeout_ms = 10000;     // 单次 send 的最长等待；等不到按 Timeout 处理
};

// 裸 TCP。Windows 用 select()，Linux 用 poll()，分别放在 transport_win.cpp / transport_posix.cpp。
// connect() 成功返回 Error::None，预算用完返回 Timeout，其它失败返回 Closed。
class TcpTransport : public Transport {
public:
    TcpTransport(std::string host, uint16_t port, TcpTransportOptions options = TcpTransportOptions());
    ~TcpTransport() override;

    TcpTransport(const TcpTransport&) = delete;
    TcpTransport& operator=(const TcpTransport&) = delete;

    Error connect() override;
    Result<size_t> read(uint8_t* buffer, size_t capacity) override;
    Error write(const uint8_t* data, size_t size) override;
    void close() override;

private:
    std::string host_;
    uint16_t port_;
    TcpTransportOptions options_;
    intptr_t handle_ = -1;  // Windows SOCKET / POSIX fd
};

// 单调毫秒时钟（相对进程内第一次调用），pump() 的时间来源
inline uint64_t monotonic_ms() {
    using clock = std::chrono::steady_clock;
    static const clock::time_point start = clock::now();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count());
}

} // namespace mqttc
