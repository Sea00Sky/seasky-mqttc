#pragma once

// 客户端状态机：连接、keepalive、自动重连、重订阅、QoS1 重发。
// 单线程：所有时间由 pump(now_ms) 驱动，调用方自己决定 pump 的频率。

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "mqttc/packet.hpp"
#include "mqttc/result.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

namespace mqttc {

class Client {
public:
    Client(Transport& transport, ClientOptions options, MessageHandler handler = MessageHandler());

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // 发起连接（不等 CONNACK）；失败时进入退避，等下一次 pump()
    Error start(uint64_t now_ms);
    // 推进状态机：处理定时器、重连、读数据、分帧、派发
    void pump(uint64_t now_ms);
    // 发 DISCONNECT 并关闭连接
    Error stop();

    // 未连接时返回 Closed；主题名非法返回 ProtocolError
    Error publish(const std::string& topic, ByteVec payload, QoS qos = QoS::AtMostOnce,
                  bool retain = false);
    // 未连接时只登记，连上（含重连）后自动发送；过滤器非法返回 ProtocolError
    Error subscribe(std::string_view filter, QoS qos = QoS::AtMostOnce);
    Error unsubscribe(std::string_view filter);

    State state() const { return state_; }
    const Stats& stats() const { return stats_; }
    const ClientOptions& options() const { return options_; }

    size_t inflight_count() const { return inflight_.size(); }
    size_t subscription_count() const { return subscriptions_.size(); }
    uint64_t next_attempt_ms() const { return next_attempt_ms_; }

    // State::Failed 的原因：稳定的短标签（connack_rejected / connect_failed …）。
    // 没发生过永久失败时返回 nullptr。detail 是与标签配套的数字（CONNACK 返回码、尝试次数）。
    const char* failure_reason() const { return failure_reason_; }
    uint64_t failure_detail() const { return failure_detail_; }

private:
    struct Inflight {
        uint16_t packet_id = 0;
        std::string topic;
        ByteVec payload;
        QoS qos = QoS::AtMostOnce;
        bool retain = false;
        uint64_t sent_ms = 0;
    };

    struct PendingSub {
        uint16_t packet_id = 0;
        std::string filter;
    };

    void begin_connect(uint64_t now_ms);
    // reason 是给日志用的短标签（connect_failed / peer_closed / malformed_packet …）
    void schedule_reconnect(uint64_t now_ms, const char* reason);
    // 判永久失败：停止自动重试，进 State::Failed（调用方可以 start() 再来一轮）
    void enter_failed(const char* reason, uint64_t detail);
    uint32_t jittered(uint32_t base_ms);
    uint32_t next_random();
    void handle_timers(uint64_t now_ms);
    void drain_input(uint64_t now_ms);
    void parse_input(uint64_t now_ms);

    void handle_packet(Packet packet, uint64_t now_ms);
    void handle_connack(const ConnAck& packet, uint64_t now_ms);
    void handle_publish(Publish packet, uint64_t now_ms);
    void handle_puback(const PubAck& packet);
    void handle_suback(const SubAck& packet);
    void handle_unsuback(const UnsubAck& packet);

    void resubscribe(uint64_t now_ms);
    void resend_inflight(uint64_t now_ms);
    bool resend_one(Inflight& item, uint64_t now_ms);

    Error send_connect(uint64_t now_ms);
    Error send_packet(const ByteVec& bytes, uint64_t now_ms);
    Error send_subscribe(std::string_view filter, QoS qos, uint64_t now_ms);
    void send_or_reconnect(const ByteVec& bytes, uint64_t now_ms);

    // 掉线时把"没等到 SUBACK/UNSUBACK 的 id"统一还回去
    void clear_pending_requests();
    uint16_t allocate_packet_id();
    void release_packet_id(uint16_t packet_id);

    // 日志：级别不够时连字符串都不拼（调用方先用 log_enabled 判一次）
    bool log_enabled(LogLevel level) const { return options_.logger.enabled(level); }
    void log(LogLevel level, LogLine line) const;

    // 接收缓冲：已消费的部分用游标表示，必要时才搬一次剩余字节
    size_t pending_bytes() const { return incoming_.size() - incoming_pos_; }
    const uint8_t* pending_data() const { return incoming_.data() + incoming_pos_; }
    void compact_input();
    void reset_input();

    Transport& transport_;
    ClientOptions options_;
    MessageHandler handler_;

    State state_ = State::Idle;
    bool running_ = false;
    uint64_t now_ms_ = 0;

    ByteVec incoming_;
    size_t incoming_pos_ = 0;  // incoming_ 里已经消费掉的字节数
    std::vector<Subscription> subscriptions_;
    std::vector<Inflight> inflight_;
    std::vector<PendingSub> pending_subs_;
    // 等 UNSUBACK 的 id：按 id 查，不按顺序扫（顺序扫在"发得多、ACK 回得慢"时会变成 O(n²)）
    std::unordered_set<uint16_t> pending_unsubs_;
    std::vector<bool> packet_id_used_ = std::vector<bool>(65536, false);
    uint32_t packet_id_free_ = 65535;  // 1..65535 可用，index 0 不用
    uint16_t next_packet_id_ = 1;

    uint64_t last_sent_ms_ = 0;
    uint64_t ping_sent_ms_ = 0;
    bool ping_outstanding_ = false;
    uint64_t connack_deadline_ms_ = 0;
    uint64_t next_attempt_ms_ = 0;
    uint32_t backoff_ms_ = 0;
    size_t attempts_ = 0;
    const char* failure_reason_ = nullptr;  // 只存字面量：调用点传的全是静态字符串
    uint64_t failure_detail_ = 0;
    uint64_t rng_state_ = 0;

    Stats stats_;
};

} // namespace mqttc
