#pragma once

// 测试里共用的小工具：拼服务端报文、把客户端推进到某个状态。
// 只依赖 InMemoryTransport（确定性、不碰网络、不等真实时间）。

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <variant>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/packet.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

namespace support {

inline mqttc::ClientOptions options_with(const std::string& client_id = "tester") {
    mqttc::ClientOptions options;
    options.client_id = client_id;
    options.keepalive = 2;  // 2 秒；测试里靠显式时间戳推进，不 sleep
    return options;
}

// 把结构体编成字节；失败就返回空（测试里当成"构造不出来"的信号）
template <typename Packet>
inline mqttc::ByteVec bytes_of(const Packet& packet) {
    auto encoded = mqttc::encode(packet);
    return encoded.ok() ? encoded.take() : mqttc::ByteVec{};
}

inline mqttc::ByteVec connack(uint8_t return_code, bool session_present = false) {
    mqttc::ConnAck packet;
    packet.return_code = return_code;
    packet.session_present = session_present;
    return bytes_of(packet);
}

inline mqttc::ByteVec puback(uint16_t packet_id) {
    mqttc::PubAck packet;
    packet.packet_id = packet_id;
    return bytes_of(packet);
}

inline mqttc::ByteVec unsuback(uint16_t packet_id) {
    mqttc::UnsubAck packet;
    packet.packet_id = packet_id;
    return bytes_of(packet);
}

inline mqttc::ByteVec suback(uint16_t packet_id, const std::vector<uint8_t>& codes) {
    mqttc::SubAck packet;
    packet.packet_id = packet_id;
    packet.return_codes = codes;
    return bytes_of(packet);
}

// 推进到 Connected：start() 发 CONNECT，喂一个 CONNACK，再 pump 一次
inline void connect(mqttc::InMemoryTransport& transport, mqttc::Client& client,
                    bool session_present = false, uint64_t now_ms = 0) {
    client.start(now_ms);
    transport.feed(connack(0x00, session_present));
    client.pump(now_ms);
}

// 取写出去的、带 packet id 的那个请求报文的 id。
// 注意不能用"最后两个字节"这种土办法：UNSUBSCRIBE 的 id 在主题过滤器前面。
inline uint16_t last_request_packet_id(const mqttc::ByteVec& written) {
    auto decoded = mqttc::decode(written);
    if (!decoded.ok()) return 0;

    const mqttc::Packet& packet = decoded.value();
    if (const auto* value = std::get_if<mqttc::Unsubscribe>(&packet)) return value->packet_id;
    if (const auto* value = std::get_if<mqttc::Subscribe>(&packet)) return value->packet_id;
    if (const auto* value = std::get_if<mqttc::Publish>(&packet)) return value->packet_id;
    return 0;
}

// 线程安全的假传输，给 AsyncClient（多线程）的测试用。
//
// 关键点：read() 在没有数据时会**真的等**一小会儿（用 cv），模拟真实 socket 上
// "poll 超时后返回 WouldBlock"的行为。这样 IO 线程确实会阻塞在 read 上，stop()
// 是不是界内返回就测得出；而 feed() 会立刻把它唤醒，所以测试仍然是确定的。
class MutexInMemoryTransport : public mqttc::Transport {
public:
    explicit MutexInMemoryTransport(uint32_t read_block_ms = 5)
        : read_block_ms_(read_block_ms) {}

    mqttc::Error connect() override {
        std::lock_guard<std::mutex> lock(mutex_);
        ++connect_calls_;
        if (fail_connect_) return mqttc::Error::Closed;
        connected_ = true;
        closed_ = false;
        incoming_.clear();
        return mqttc::Error::None;
    }

    mqttc::Result<size_t> read(uint8_t* buffer, size_t capacity) override {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!connected_) return mqttc::Result<size_t>::failure(mqttc::Error::Closed);
        if (incoming_.empty() && !closed_) {
            cv_.wait_for(lock, std::chrono::milliseconds(read_block_ms_), [this] {
                return !incoming_.empty() || closed_ || !connected_;
            });
        }
        if (!incoming_.empty()) {
            const size_t count = std::min(capacity, incoming_.size());
            std::copy_n(incoming_.begin(), static_cast<std::ptrdiff_t>(count), buffer);
            incoming_.erase(incoming_.begin(), incoming_.begin() + static_cast<std::ptrdiff_t>(count));
            return mqttc::Result<size_t>::success(count);
        }
        return mqttc::Result<size_t>::failure(closed_ ? mqttc::Error::Closed : mqttc::Error::WouldBlock);
    }

    mqttc::Error write(const uint8_t* data, size_t size) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ || closed_) return mqttc::Error::Closed;

        if (size == 2 && data[0] == 0xC0 && data[1] == 0x00) {  // PINGREQ
            pingreq_times_ms_.push_back(mqttc::monotonic_ms());
            if (auto_pingresp_) {
                incoming_.push_back(0xD0);
                incoming_.push_back(0x00);
            }
        }
        outgoing_.insert(outgoing_.end(), data, data + size);
        cv_.notify_all();
        return mqttc::Error::None;
    }

    void close() override {
        std::lock_guard<std::mutex> lock(mutex_);
        connected_ = false;
        incoming_.clear();
        cv_.notify_all();
    }

    // ---- 测试控制（线程安全）----

    void feed(const uint8_t* data, size_t size) {
        std::lock_guard<std::mutex> lock(mutex_);
        incoming_.insert(incoming_.end(), data, data + size);
        cv_.notify_all();
    }
    void feed(const mqttc::ByteVec& bytes) { feed(bytes.data(), bytes.size()); }

    void set_closed(bool value) {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = value;
        cv_.notify_all();
    }
    void set_connect_fails(bool value) {
        std::lock_guard<std::mutex> lock(mutex_);
        fail_connect_ = value;
    }
    void set_auto_pingresp(bool value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto_pingresp_ = value;
    }
    void set_read_block_ms(uint32_t value) {
        std::lock_guard<std::mutex> lock(mutex_);
        read_block_ms_ = value;
    }

    mqttc::ByteVec written() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return outgoing_;
    }
    void clear_written() {
        std::lock_guard<std::mutex> lock(mutex_);
        outgoing_.clear();
    }
    size_t connect_calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return connect_calls_;
    }
    std::vector<uint64_t> pingreq_times_ms() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return pingreq_times_ms_;
    }
    size_t pending_incoming() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return incoming_.size();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    mqttc::ByteVec incoming_;
    mqttc::ByteVec outgoing_;
    std::vector<uint64_t> pingreq_times_ms_;
    uint32_t read_block_ms_ = 5;
    size_t connect_calls_ = 0;
    bool connected_ = false;
    bool closed_ = false;
    bool fail_connect_ = false;
    bool auto_pingresp_ = false;
};

} // namespace support
