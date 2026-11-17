#include "mqttc/client.hpp"

#include <algorithm>
#include <utility>

#include "mqttc/topic.hpp"

namespace mqttc {
namespace {

// 等 CONNACK 的上限；超时按连接失败处理
constexpr uint64_t kConnAckTimeoutMs = 10000;

constexpr uint16_t kMaxPacketId = 65535;

// CONNACK 返回码里"再试也没用"的那几个：1 版本不支持、2 标识符被拒、
// 4 凭据错、5 未授权。只有 3（服务不可用）值得重试。
bool is_permanent_connack(uint8_t return_code) {
    return return_code == 1 || return_code == 2 || return_code == 4 || return_code == 5;
}

} // namespace

Client::Client(Transport& transport, ClientOptions options, MessageHandler handler)
    : transport_(transport), options_(std::move(options)), handler_(std::move(handler)) {
    // 退避抖动用的随机源：给了种子就完全可复现（测试用），否则按时间播一次
    rng_state_ = options_.rng_seed != 0
                     ? options_.rng_seed
                     : (monotonic_ms() * 0x2545F4914F6CDD1Dull) ^ 0x9E3779B97F4A7C15ull;
    if (rng_state_ == 0) rng_state_ = 0x2545F4914F6CDD1Dull;  // xorshift 不能是 0
}

// ---------------------------------------------------------------- 对外接口

Error Client::start(uint64_t now_ms) {
    running_ = true;
    state_ = State::Idle;
    backoff_ms_ = 0;
    attempts_ = 0;
    failure_reason_ = nullptr;
    failure_detail_ = 0;
    next_attempt_ms_ = now_ms;
    pump(now_ms);

    if (state_ == State::WaitingConnAck || state_ == State::Connected) return Error::None;
    return Error::Closed;
}

// ---------------------------------------------------------------- 日志

void Client::log(LogLevel level, LogLine line) const {
    if (!log_enabled(level)) return;
    options_.logger.log(level, line.take());
}

Error Client::stop() {
    running_ = false;
    Error error = Error::None;
    if (state_ == State::Connected || state_ == State::WaitingConnAck) {
        auto encoded = encode(Disconnect{});
        if (encoded) error = send_packet(encoded.value(), now_ms_);
    }
    transport_.close();
    state_ = State::Idle;
    ping_outstanding_ = false;
    reset_input();
    return error;
}

Error Client::publish(const std::string& topic, ByteVec payload, QoS qos, bool retain) {
    if (state_ != State::Connected) return Error::Closed;

    if (qos != QoS::AtMostOnce && inflight_.size() >= options_.max_inflight) {
        return Error::Busy;  // 到自己的在途上限了，调用方该退让（不要再往库里堆）
    }

    Publish packet;
    packet.topic = topic;
    packet.payload = std::move(payload);
    packet.qos = static_cast<uint8_t>(qos);
    packet.retain = retain;

    if (packet.qos > 0) {
        packet.packet_id = allocate_packet_id();
        if (packet.packet_id == 0) return Error::WouldBlock;  // 65535 条在途，暂时发不出
    }

    auto encoded = encode(packet);
    if (!encoded) return encoded.error();

    if (packet.qos > 0) {
        Inflight item;
        item.packet_id = packet.packet_id;
        item.topic = packet.topic;
        item.payload = packet.payload;
        item.qos = qos;
        item.retain = retain;
        item.sent_ms = now_ms_;
        inflight_.push_back(std::move(item));
    }

    const Error error = send_packet(encoded.value(), now_ms_);
    if (error == Error::None) ++stats_.published;
    return error;
}

Error Client::subscribe(std::string_view filter, QoS qos) {
    if (!valid_topic_filter(filter)) return Error::ProtocolError;

    bool known = false;
    for (Subscription& item : subscriptions_) {
        if (item.filter == filter) {
            item.qos = static_cast<uint8_t>(qos);
            known = true;
        }
    }
    if (!known) subscriptions_.push_back(Subscription{std::string(filter), static_cast<uint8_t>(qos)});

    if (known) return Error::None;  // 已订阅过；改 QoS 只在重连后生效（clean session）
    if (state_ != State::Connected) return Error::None;
    return send_subscribe(filter, qos, now_ms_);
}

Error Client::unsubscribe(std::string_view filter) {
    if (!valid_topic_filter(filter)) return Error::ProtocolError;

    subscriptions_.erase(std::remove_if(subscriptions_.begin(), subscriptions_.end(),
                                        [filter](const Subscription& item) {
                                            return item.filter == filter;
                                        }),
                        subscriptions_.end());

    if (state_ != State::Connected) return Error::None;

    Unsubscribe packet;
    packet.packet_id = allocate_packet_id();
    if (packet.packet_id == 0) return Error::WouldBlock;
    packet.filters.emplace_back(filter);

    auto encoded = encode(packet);
    if (!encoded) {
        release_packet_id(packet.packet_id);
        return encoded.error();
    }

    // 登记下来，等 UNSUBACK 到了再还 id（掉线时由 clear_pending_requests 统一归还）
    pending_unsubs_.insert(packet.packet_id);
    return send_packet(encoded.value(), now_ms_);
}

// ---------------------------------------------------------------- 状态机

void Client::pump(uint64_t now_ms) {
    if (!running_) return;
    now_ms_ = now_ms;
    ++stats_.ticks;

    if (state_ == State::Idle) {
        begin_connect(now_ms);
    } else if (state_ == State::Backoff && now_ms >= next_attempt_ms_) {
        begin_connect(now_ms);
    } else if (state_ == State::WaitingConnAck && now_ms >= connack_deadline_ms_) {
        schedule_reconnect(now_ms, "connack_timeout");
    } else if (state_ == State::Connected) {
        handle_timers(now_ms);
    }

    drain_input(now_ms);
}

void Client::begin_connect(uint64_t now_ms) {
    // 这里就计数：max_connect_attempts 管的是"试了几次"，连不上 socket 也算一次，
    // 不然黑洞地址会让重试永远停不下来
    ++attempts_;

    transport_.close();
    const Error error = transport_.connect();
    if (error != Error::None) {
        log(LogLevel::Warn, LogLine("connect_failed").field("error", error_name(error)));
        // 证书校验失败不是"网络抖动"：同样的 CA/证书再试多少次都是同样的结果。
        // 除非调用方显式要求对永久失败也重试，否则直接进 Failed，不再无限打。
        if (error == Error::CertVerify && !options_.retry_on_permanent) {
            enter_failed("cert_verify_failed", 0);
            return;
        }
        schedule_reconnect(now_ms, "connect_failed");
        return;
    }

    reset_input();
    clear_pending_requests();
    ping_outstanding_ = false;

    if (send_connect(now_ms) != Error::None) {
        schedule_reconnect(now_ms, "connect_send_failed");
        return;
    }

    state_ = State::WaitingConnAck;
    connack_deadline_ms_ = now_ms + kConnAckTimeoutMs;
}

void Client::schedule_reconnect(uint64_t now_ms, const char* reason) {
    transport_.close();
    reset_input();
    clear_pending_requests();
    ping_outstanding_ = false;

    // 试满次数就判永久失败，不再无限重试（对端省事，调用方也能拿到明确信号）
    if (options_.max_connect_attempts != 0 && attempts_ >= options_.max_connect_attempts) {
        enter_failed(reason, attempts_);
        return;
    }

    state_ = State::Backoff;

    if (backoff_ms_ == 0) {
        backoff_ms_ = options_.reconnect_min_ms;
    } else {
        backoff_ms_ = std::min<uint32_t>(backoff_ms_ * 2, options_.reconnect_max_ms);
    }
    const uint32_t delay_ms = jittered(backoff_ms_);
    next_attempt_ms_ = now_ms + delay_ms;

    log(LogLevel::Info, LogLine("reconnect_scheduled")
                            .field("reason", reason)
                            .field("backoff_ms", backoff_ms_)
                            .field("delay_ms", delay_ms)
                            .field("attempt", static_cast<uint64_t>(attempts_)));
}

void Client::enter_failed(const char* reason, uint64_t detail) {
    transport_.close();
    reset_input();
    clear_pending_requests();
    ping_outstanding_ = false;
    state_ = State::Failed;
    failure_reason_ = reason;
    failure_detail_ = detail;
    ++stats_.permanent_failures;

    log(LogLevel::Error, LogLine("permanent_failure")
                             .field("reason", reason)
                             .field("detail", detail)
                             .field("attempts", static_cast<uint64_t>(attempts_)));
}

// 退避抖动：base × (1 ± ratio)，用可复现的 xorshift。
// 目的只有一个——别让一批同时掉线的设备在同一毫秒一起回来。
uint32_t Client::jittered(uint32_t base_ms) {
    const double ratio = options_.backoff_jitter_ratio;
    if (ratio <= 0.0) return base_ms;

    const double spread = static_cast<double>(base_ms) * (ratio > 1.0 ? 1.0 : ratio);
    // 均匀取 [-spread, +spread]
    const double unit = static_cast<double>(next_random()) / 4294967296.0;  // [0,1)
    const double offset = (unit * 2.0 - 1.0) * spread;
    const double value = static_cast<double>(base_ms) + offset;
    if (value < 0.0) return 0;
    return static_cast<uint32_t>(value);
}

uint32_t Client::next_random() {
    // xorshift64*：够用、够快、不引 <random>
    uint64_t x = rng_state_;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    rng_state_ = x;
    return static_cast<uint32_t>((x * 0x2545F4914F6CDD1Dull) >> 32);
}

void Client::handle_timers(uint64_t now_ms) {
    const uint64_t keepalive_ms = static_cast<uint64_t>(options_.keepalive) * 1000;

    if (keepalive_ms != 0) {
        if (ping_outstanding_ && now_ms - ping_sent_ms_ >= keepalive_ms) {
            ++stats_.keepalive_timeouts;  // 等不到 PINGRESP，判链路已死
            log(LogLevel::Warn, LogLine("keepalive_timeout")
                                    .field("keepalive_s", options_.keepalive));
            schedule_reconnect(now_ms, "keepalive_timeout");
            return;
        }
        if (!ping_outstanding_ && now_ms - last_sent_ms_ >= keepalive_ms) {
            auto encoded = encode(PingReq{});
            if (!encoded) {
                schedule_reconnect(now_ms, "ping_encode_failed");
                return;
            }
            if (send_packet(encoded.value(), now_ms) != Error::None) {
                schedule_reconnect(now_ms, "ping_send_failed");
                return;
            }
            ping_outstanding_ = true;
            ping_sent_ms_ = now_ms;
        }
    }

    for (Inflight& item : inflight_) {
        if (now_ms - item.sent_ms >= options_.publish_retry_ms) {
            if (!resend_one(item, now_ms)) {
                schedule_reconnect(now_ms, "resend_failed");
                return;
            }
        }
    }
}

void Client::drain_input(uint64_t now_ms) {
    if (state_ != State::WaitingConnAck && state_ != State::Connected) return;

    bool peer_closed = false;
    uint8_t buffer[4096];
    for (;;) {
        auto read = transport_.read(buffer, sizeof(buffer));
        if (!read) {
            if (read.error() == Error::Closed) {
                // 不能在这里立刻重连：对端常常是"先发最后一包（比如 CONNACK 错误码）
                // 再关连接"，刚读到的字节还堆在缓冲里没有解析。先解析完再判关闭。
                peer_closed = true;
            }
            break;  // WouldBlock：这一次没有更多数据
        }
        stats_.bytes_in += read.value();
        incoming_.insert(incoming_.end(), buffer, buffer + read.value());
        if (pending_bytes() > options_.max_packet_bytes) {
            ++stats_.oversize_packets;
            log(LogLevel::Warn, LogLine("oversize_input")
                                    .field("buffered_bytes", static_cast<uint64_t>(pending_bytes()))
                                    .field("limit_bytes", options_.max_packet_bytes));
            schedule_reconnect(now_ms, "oversize_buffer");
            return;
        }
    }

    parse_input(now_ms);

    // 解析没让状态离开"在网"（没有掉线重连、没有判永久失败），那对端就是真关了
    if (peer_closed && (state_ == State::WaitingConnAck || state_ == State::Connected)) {
        schedule_reconnect(now_ms, "peer_closed");
    }
}

void Client::parse_input(uint64_t now_ms) {
    for (;;) {
        const size_t available = pending_bytes();
        if (available < 2) {
            compact_input();
            return;
        }

        auto total = peek_packet_size(pending_data(), available);
        if (!total) {
            if (total.error() == Error::WouldBlock) {  // 半个报文，等下一次
                compact_input();
                return;
            }
            ++stats_.protocol_errors;
            log(LogLevel::Warn, LogLine("malformed_length").field("error", error_name(total.error())));
            schedule_reconnect(now_ms, "malformed_length");
            return;
        }
        // 协议上限是 256 MiB，但那是"对端说了算"的数字：超过自己的上限就当恶意/不可用处理
        if (total.value() > options_.max_packet_bytes || total.value() > kMaxPacketSize) {
            ++stats_.oversize_packets;
            log(LogLevel::Warn, LogLine("oversize_packet")
                                    .field("packet_bytes", static_cast<uint64_t>(total.value()))
                                    .field("limit_bytes", options_.max_packet_bytes));
            schedule_reconnect(now_ms, "oversize_packet");
            return;
        }
        if (available < total.value()) {
            compact_input();
            return;
        }

        auto packet = decode(pending_data(), total.value());
        incoming_pos_ += total.value();
        if (!packet) {
            ++stats_.protocol_errors;
            log(LogLevel::Warn, LogLine("malformed_packet")
                                    .field("error", error_name(packet.error()))
                                    .field("packet_bytes", static_cast<uint64_t>(total.value())));
            schedule_reconnect(now_ms, "malformed_packet");
            return;
        }

        handle_packet(packet.take(), now_ms);
        if (state_ != State::WaitingConnAck && state_ != State::Connected) return;  // 处理中掉线
    }
}

// 已消费的字节不再每包搬一次：攒够了（或攒到一半）才把剩余字节挪到前面
void Client::compact_input() {
    if (incoming_pos_ == 0) return;
    if (incoming_pos_ < 4096 && incoming_pos_ * 2 < incoming_.size()) return;
    incoming_.erase(incoming_.begin(),
                    incoming_.begin() + static_cast<std::ptrdiff_t>(incoming_pos_));
    incoming_pos_ = 0;
}

void Client::reset_input() {
    incoming_.clear();
    incoming_pos_ = 0;
}

// ---------------------------------------------------------------- 收包处理

void Client::handle_packet(Packet packet, uint64_t now_ms) {
    if (auto* connack = std::get_if<ConnAck>(&packet)) {
        handle_connack(*connack, now_ms);
        return;
    }
    if (auto* publish = std::get_if<Publish>(&packet)) {
        handle_publish(std::move(*publish), now_ms);
        return;
    }
    if (auto* puback = std::get_if<PubAck>(&packet)) {
        handle_puback(*puback);
        return;
    }
    if (auto* suback = std::get_if<SubAck>(&packet)) {
        handle_suback(*suback);
        return;
    }
    if (auto* unsuback = std::get_if<UnsubAck>(&packet)) {
        handle_unsuback(*unsuback);
        return;
    }
    if (std::holds_alternative<PingResp>(packet)) {
        ping_outstanding_ = false;
        return;
    }
    ++stats_.unexpected_packets;  // 服务端不该发 CONNECT/SUBSCRIBE/PINGREQ/DISCONNECT
}

void Client::handle_connack(const ConnAck& packet, uint64_t now_ms) {
    if (state_ != State::WaitingConnAck) {
        ++stats_.unexpected_packets;
        return;
    }
    if (packet.return_code != 0) {
        log(LogLevel::Error, LogLine("connack_rejected")
                                 .field("return_code", static_cast<uint64_t>(packet.return_code))
                                 .field("permanent", is_permanent_connack(packet.return_code)));
        // 版本不支持 / 标识符被拒 / 凭据错 / 未授权：再试还是同样的结果，不该一直打
        if (!options_.retry_on_permanent && is_permanent_connack(packet.return_code)) {
            enter_failed("connack_rejected", packet.return_code);
            return;
        }
        schedule_reconnect(now_ms, "connack_rejected");
        return;
    }

    state_ = State::Connected;
    if (attempts_ > 1) ++stats_.reconnects;
    backoff_ms_ = 0;
    ping_outstanding_ = false;
    last_sent_ms_ = now_ms;

    log(LogLevel::Info, LogLine("connected")
                            .field("client_id", options_.client_id)
                            .field("session_present", packet.session_present)
                            .field("attempt", static_cast<uint64_t>(attempts_))
                            .field("reconnects", stats_.reconnects));

    resubscribe(now_ms);
    resend_inflight(now_ms);
}

void Client::handle_publish(Publish packet, uint64_t now_ms) {
    ++stats_.received;

    Message message;
    message.topic = std::move(packet.topic);
    message.payload = std::move(packet.payload);
    message.qos = packet.qos == 1 ? QoS::AtLeastOnce : QoS::AtMostOnce;
    message.retain = packet.retain;

    bool accepted = true;
    if (handler_) accepted = handler_(message);
    if (!accepted) {
        ++stats_.dropped;
        if (log_enabled(LogLevel::Debug)) {
            log(LogLevel::Debug, LogLine("message_dropped").field("topic", message.topic));
        }
    }

    // QoS1 仍然回 PUBACK：协议层已经收下，丢弃只是应用层的选择，
    // 不回 ACK 会让 broker 一直重发。
    if (packet.qos == 1) {
        PubAck ack;
        ack.packet_id = packet.packet_id;
        auto encoded = encode(ack);
        if (encoded) send_or_reconnect(encoded.value(), now_ms);
    }
}

void Client::handle_puback(const PubAck& packet) {
    for (size_t i = 0; i < inflight_.size(); ++i) {
        if (inflight_[i].packet_id == packet.packet_id) {
            release_packet_id(inflight_[i].packet_id);
            inflight_.erase(inflight_.begin() + static_cast<std::ptrdiff_t>(i));
            return;
        }
    }
}

void Client::handle_suback(const SubAck& packet) {
    for (uint8_t code : packet.return_codes) {
        if (code == 0x80) ++stats_.subscribe_failures;
    }

    // 一条 SUBSCRIBE 可以带多个过滤器，共用一个 packet id，所以这里要把同 id 的全清掉
    bool matched = false;
    for (size_t i = 0; i < pending_subs_.size();) {
        if (pending_subs_[i].packet_id == packet.packet_id) {
            matched = true;
            pending_subs_.erase(pending_subs_.begin() + static_cast<std::ptrdiff_t>(i));
        } else {
            ++i;
        }
    }

    // 只有确实是我们登记过的 id 才能还：否则一条 id 对不上的 SUBACK 会把
    // 正在途中的 QoS1 消息的 id 提前放出去，后面复用同一个 id 就串了
    if (!matched) {
        ++stats_.unexpected_packets;
        return;
    }
    release_packet_id(packet.packet_id);
}

void Client::handle_unsuback(const UnsubAck& packet) {
    if (pending_unsubs_.erase(packet.packet_id) == 0) {
        ++stats_.unexpected_packets;  // 同上：没登记过的 id 不是我们的，不能去还
        return;
    }
    release_packet_id(packet.packet_id);
}

// ---------------------------------------------------------------- 重订阅与重发

void Client::resubscribe(uint64_t now_ms) {
    if (subscriptions_.empty()) return;

    Subscribe packet;
    packet.packet_id = allocate_packet_id();
    if (packet.packet_id == 0) return;
    packet.entries = subscriptions_;

    auto encoded = encode(packet);
    if (!encoded) {
        release_packet_id(packet.packet_id);
        return;
    }
    for (const Subscription& entry : packet.entries) {
        pending_subs_.push_back(PendingSub{packet.packet_id, entry.filter});
    }
    send_or_reconnect(encoded.value(), now_ms);
}

void Client::resend_inflight(uint64_t now_ms) {
    for (Inflight& item : inflight_) {
        if (!resend_one(item, now_ms)) return;
    }
}

bool Client::resend_one(Inflight& item, uint64_t now_ms) {
    Publish packet;
    packet.topic = item.topic;
    packet.payload = item.payload;
    packet.qos = static_cast<uint8_t>(item.qos);
    packet.retain = item.retain;
    packet.dup = true;  // 重发一律带 DUP
    packet.packet_id = item.packet_id;

    auto encoded = encode(packet);
    if (!encoded) return false;
    if (send_packet(encoded.value(), now_ms) != Error::None) return false;

    item.sent_ms = now_ms;
    ++stats_.resends;
    return true;
}

// ---------------------------------------------------------------- 发送与 packet id

Error Client::send_connect(uint64_t now_ms) {
    Connect packet;
    packet.client_id = options_.client_id;
    packet.clean_session = options_.clean_session;
    packet.keepalive = options_.keepalive;
    packet.will = options_.will;
    packet.username = options_.username;
    packet.password = options_.password;

    auto encoded = encode(packet);
    if (!encoded) return encoded.error();
    return send_packet(encoded.value(), now_ms);
}

Error Client::send_packet(const ByteVec& bytes, uint64_t now_ms) {
    const Error error = transport_.write(bytes.data(), bytes.size());
    if (error != Error::None) return error;
    stats_.bytes_out += bytes.size();
    last_sent_ms_ = now_ms;  // 任何报文都算活动，用于 keepalive 计时
    return Error::None;
}

void Client::send_or_reconnect(const ByteVec& bytes, uint64_t now_ms) {
    if (send_packet(bytes, now_ms) != Error::None) schedule_reconnect(now_ms, "send_failed");
}

Error Client::send_subscribe(std::string_view filter, QoS qos, uint64_t now_ms) {
    Subscribe packet;
    packet.packet_id = allocate_packet_id();
    if (packet.packet_id == 0) return Error::WouldBlock;
    packet.entries.push_back(Subscription{std::string(filter), static_cast<uint8_t>(qos)});

    auto encoded = encode(packet);
    if (!encoded) {
        release_packet_id(packet.packet_id);
        return encoded.error();
    }
    pending_subs_.push_back(PendingSub{packet.packet_id, std::string(filter)});
    return send_packet(encoded.value(), now_ms);
}

// 掉线时把"没等到 SUBACK / UNSUBACK 的 id"还回去，否则重连几次就把 id 耗光
void Client::clear_pending_requests() {
    for (const PendingSub& sub : pending_subs_) release_packet_id(sub.packet_id);
    pending_subs_.clear();
    for (uint16_t packet_id : pending_unsubs_) release_packet_id(packet_id);
    pending_unsubs_.clear();
}

// 位图 + 游标轮转。空闲计数只是为了"表满时立刻返回 0"，不再白扫 65535 次
uint16_t Client::allocate_packet_id() {
    if (packet_id_free_ == 0) return 0;  // 全部在用
    for (;;) {
        const uint16_t candidate = next_packet_id_;
        next_packet_id_ = next_packet_id_ >= kMaxPacketId
                              ? static_cast<uint16_t>(1)
                              : static_cast<uint16_t>(next_packet_id_ + 1);
        if (!packet_id_used_[candidate]) {
            packet_id_used_[candidate] = true;
            --packet_id_free_;
            return candidate;
        }
    }
}

void Client::release_packet_id(uint16_t packet_id) {
    if (packet_id == 0 || !packet_id_used_[packet_id]) return;
    packet_id_used_[packet_id] = false;
    ++packet_id_free_;
}

} // namespace mqttc
