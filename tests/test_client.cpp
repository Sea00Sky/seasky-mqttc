// 客户端状态机行为测试：全部走 InMemoryTransport，时间由参数注入，不碰真 socket。

#include <string>
#include <variant>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"
#include "test.hpp"

namespace {

using mqttc::ByteVec;

mqttc::ClientOptions default_options() {
    mqttc::ClientOptions options;
    options.client_id = "tester";
    options.keepalive = 2;  // 2000 ms
    options.publish_retry_ms = 1000;
    options.reconnect_min_ms = 100;
    options.reconnect_max_ms = 400;
    // 抖动默认关掉：这个文件里断言的是精确的 next_attempt_ms()。
    // 抖动本身的可复现测试在 test_reconnect.cpp 里，用显式 rng_seed 打开。
    options.backoff_jitter_ratio = 0.0;
    return options;
}

ByteVec connack_bytes(uint8_t code) { return {0x20, 0x02, 0x00, code}; }
ByteVec pingresp_bytes() { return {0xD0, 0x00}; }

ByteVec puback_bytes(uint16_t id) {
    return {0x40, 0x02, static_cast<uint8_t>(id >> 8), static_cast<uint8_t>(id & 0xFF)};
}

ByteVec suback_bytes(uint16_t id, uint8_t code) {
    return {0x90, 0x03, static_cast<uint8_t>(id >> 8), static_cast<uint8_t>(id & 0xFF), code};
}

// 手工构造 broker 发来的 PUBLISH（小报文，Remaining Length 单字节够用）
ByteVec publish_in(const std::string& topic, const std::string& payload, uint8_t qos, uint16_t id) {
    ByteVec body;
    body.push_back(static_cast<uint8_t>(topic.size() >> 8));
    body.push_back(static_cast<uint8_t>(topic.size() & 0xFF));
    body.insert(body.end(), topic.begin(), topic.end());
    if (qos > 0) {
        body.push_back(static_cast<uint8_t>(id >> 8));
        body.push_back(static_cast<uint8_t>(id & 0xFF));
    }
    body.insert(body.end(), payload.begin(), payload.end());

    ByteVec out;
    out.push_back(static_cast<uint8_t>(0x30 | (qos << 1)));
    out.push_back(static_cast<uint8_t>(body.size()));
    out.insert(out.end(), body.begin(), body.end());
    return out;
}

std::vector<mqttc::Packet> split_packets(const ByteVec& bytes) {
    std::vector<mqttc::Packet> out;
    size_t offset = 0;
    while (offset + 2 <= bytes.size()) {
        auto total = mqttc::peek_packet_size(bytes.data() + offset, bytes.size() - offset);
        if (!total || bytes.size() - offset < total.value()) break;
        auto packet = mqttc::decode(bytes.data() + offset, total.value());
        if (!packet) break;
        out.push_back(packet.take());
        offset += total.value();
    }
    return out;
}

template <typename T>
std::vector<T> collect(const mqttc::InMemoryTransport& transport) {
    std::vector<T> out;
    for (const mqttc::Packet& packet : split_packets(transport.written())) {
        if (const auto* item = std::get_if<T>(&packet)) out.push_back(*item);
    }
    return out;
}

// 起连接并喂 CONNACK，把客户端推进到 Connected
void connect_now(mqttc::Client& client, mqttc::InMemoryTransport& transport, uint64_t now_ms) {
    client.start(now_ms);
    transport.clear_written();
    transport.feed(connack_bytes(0x00));
    client.pump(now_ms);
}

} // namespace

TEST(client_start_sends_connect) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());

    CHECK_EQ(client.start(0), mqttc::Error::None);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));

    auto connects = collect<mqttc::Connect>(transport);
    REQUIRE(connects.size() == 1);
    CHECK_EQ(connects[0].client_id, std::string("tester"));
    CHECK(connects[0].clean_session);
    CHECK_EQ(connects[0].keepalive, 2);

    transport.clear_written();
    transport.feed(connack_bytes(0x00));
    client.pump(0);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    CHECK_EQ(client.stats().reconnects, static_cast<uint64_t>(0));
}

TEST(client_connack_transient_code_keeps_retrying) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());

    client.start(0);
    transport.feed(connack_bytes(3));  // 服务端暂时不可用：瞬时失败，值得再试
    client.pump(0);

    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));
    CHECK_EQ(client.next_attempt_ms(), static_cast<uint64_t>(100));
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(0));

    transport.clear_written();
    client.pump(99);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    client.pump(100);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(2));
    CHECK_EQ(collect<mqttc::Connect>(transport).size(), static_cast<size_t>(1));
}

TEST(client_connack_permanent_code_stops_retrying) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());

    client.start(0);
    transport.feed(connack_bytes(4));  // 用户名或密码错：再试也是同一个结果
    client.pump(0);

    CHECK_EQ(client.state(), mqttc::State::Failed);
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(1));
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));

    // 时间过去多久都不再发起连接
    for (uint64_t now = 100; now <= 10000; now += 100) {
        client.pump(now);
        CHECK_EQ(client.state(), mqttc::State::Failed);
    }
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));

    // Failed 之后 API 行为不变：没连上，publish 返回 Closed，调用方据此处理
    CHECK(client.publish("a/b", {1, 2}) == mqttc::Error::Closed);
}

TEST(client_connack_permanent_code_can_be_retried_explicitly) {
    mqttc::ClientOptions options = default_options();
    options.retry_on_permanent = true;
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);

    client.start(0);
    transport.feed(connack_bytes(5));  // 未授权
    client.pump(0);

    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(client.next_attempt_ms(), static_cast<uint64_t>(100));
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(0));
}

TEST(client_connack_permanent_code_fails_even_at_attempt_limit) {
    mqttc::ClientOptions options = default_options();
    options.max_connect_attempts = 1;
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);

    client.start(0);
    transport.feed(connack_bytes(3));  // 瞬时失败，但试满一次配额了
    client.pump(0);

    CHECK_EQ(client.state(), mqttc::State::Failed);
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(1));
}

TEST(client_transport_connect_failure_backs_off) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    transport.set_connect_fails(true);

    CHECK_EQ(client.start(0), mqttc::Error::Closed);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));
    CHECK_EQ(client.next_attempt_ms(), static_cast<uint64_t>(100));  // 第一次退避 = 下限

    client.pump(99);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    client.pump(100);  // 仍连不上
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(2));
    CHECK_EQ(client.next_attempt_ms(), static_cast<uint64_t>(300));  // 退避加倍：100 → 200

    transport.set_connect_fails(false);
    client.pump(299);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    client.pump(300);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(3));
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);
}

TEST(client_keepalive_sends_pingreq) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    client.pump(1999);
    CHECK_EQ(collect<mqttc::PingReq>(transport).size(), static_cast<size_t>(0));

    client.pump(2000);
    CHECK_EQ(collect<mqttc::PingReq>(transport).size(), static_cast<size_t>(1));

    // 收到 PINGRESP 后不应重连，也不该重复发 PINGREQ
    transport.feed(pingresp_bytes());
    client.pump(2001);
    client.pump(3000);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));
    CHECK_EQ(client.stats().keepalive_timeouts, static_cast<uint64_t>(0));
}

TEST(client_missing_pingresp_triggers_reconnect) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    client.pump(2000);  // 发 PINGREQ
    CHECK_EQ(collect<mqttc::PingReq>(transport).size(), static_cast<size_t>(1));

    client.pump(3999);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    client.pump(4000);  // 一个 keepalive 内没等到 PINGRESP
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(client.stats().keepalive_timeouts, static_cast<uint64_t>(1));

    transport.clear_written();
    client.pump(4100);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(2));
    CHECK_EQ(collect<mqttc::Connect>(transport).size(), static_cast<size_t>(1));

    transport.feed(connack_bytes(0x00));
    client.pump(4100);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    CHECK_EQ(client.stats().reconnects, static_cast<uint64_t>(1));
}

TEST(client_reconnect_resubscribes) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    CHECK_EQ(client.subscribe("a/#", mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    CHECK_EQ(client.subscription_count(), static_cast<size_t>(1));
    auto first = collect<mqttc::Subscribe>(transport);
    REQUIRE(first.size() == 1);
    const uint16_t first_id = first[0].packet_id;
    transport.feed(suback_bytes(first_id, 0x01));
    client.pump(0);

    // 掉线 → 退避 → 重连
    transport.set_closed(true);
    client.pump(10);
    CHECK_EQ(client.state(), mqttc::State::Backoff);

    transport.set_closed(false);
    transport.clear_written();
    client.pump(110);
    CHECK_EQ(collect<mqttc::Connect>(transport).size(), static_cast<size_t>(1));

    transport.clear_written();
    transport.feed(connack_bytes(0x00));
    client.pump(110);

    auto again = collect<mqttc::Subscribe>(transport);
    REQUIRE(again.size() == 1);
    CHECK_EQ(again[0].entries.size(), static_cast<size_t>(1));
    CHECK_EQ(again[0].entries[0].filter, std::string("a/#"));
    CHECK(again[0].packet_id != first_id);  // 上一轮的 id 已归还
    CHECK_EQ(client.stats().reconnects, static_cast<uint64_t>(1));
}

TEST(client_qos1_resend_sets_dup) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    CHECK_EQ(client.publish("a/b", ByteVec{'h', 'i'}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(1));

    auto sent = collect<mqttc::Publish>(transport);
    REQUIRE(sent.size() == 1);
    CHECK(!sent[0].dup);
    CHECK_EQ(sent[0].packet_id, 1);

    client.pump(999);
    CHECK_EQ(collect<mqttc::Publish>(transport).size(), static_cast<size_t>(1));

    client.pump(1000);  // 超过重发间隔
    auto resent = collect<mqttc::Publish>(transport);
    REQUIRE(resent.size() == 2);
    CHECK(resent[1].dup);
    CHECK_EQ(resent[1].packet_id, 1);
    CHECK_EQ(resent[1].payload, (ByteVec{'h', 'i'}));
    CHECK_EQ(client.stats().resends, static_cast<uint64_t>(1));

    transport.feed(puback_bytes(1));
    client.pump(1001);
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(0));

    client.pump(5000);
    CHECK_EQ(collect<mqttc::Publish>(transport).size(), static_cast<size_t>(2));  // 不再重发
}

TEST(client_qos1_resend_after_reconnect) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    CHECK_EQ(client.publish("a", ByteVec{'x'}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    transport.set_closed(true);  // 没等到 PUBACK 就断线
    client.pump(10);

    transport.set_closed(false);
    transport.clear_written();
    client.pump(110);
    transport.clear_written();
    transport.feed(connack_bytes(0x00));
    client.pump(110);

    auto resent = collect<mqttc::Publish>(transport);
    REQUIRE(resent.size() == 1);
    CHECK(resent[0].dup);
    CHECK_EQ(resent[0].packet_id, 1);
    CHECK_EQ(client.stats().resends, static_cast<uint64_t>(1));
}

TEST(client_packet_ids_unique_and_reused_after_puback) {
    mqttc::InMemoryTransport transport;
    // 这个用例就是要用满整个 id 空间（65535 个），所以把在途上限放开
    mqttc::ClientOptions options = default_options();
    options.max_inflight = 65535;
    mqttc::Client client(transport, options);
    connect_now(client, transport, 0);

    CHECK_EQ(client.publish("a", ByteVec{1}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    CHECK_EQ(client.publish("a", ByteVec{2}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    transport.feed(puback_bytes(1));
    client.pump(0);
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(1));

    // 把剩下的 id 用满（2 已在途，不回绕 1 之前不会再用到 1）
    for (int i = 0; i < 65533; ++i) {
        CHECK_EQ(client.publish("a", ByteVec{3}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    }
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(65534));

    transport.clear_written();
    CHECK_EQ(client.publish("a", ByteVec{4}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);

    auto sent = collect<mqttc::Publish>(transport);
    REQUIRE(sent.size() == 1);
    CHECK_EQ(sent[0].packet_id, 1);  // 1 已收到 PUBACK，可以复用；2 仍在途，不会被发给它
}

TEST(client_byte_by_byte_feed) {
    mqttc::InMemoryTransport transport;
    std::vector<mqttc::Message> messages;
    mqttc::Client client(transport, default_options(),
                         [&messages](const mqttc::Message& message) {
                             messages.push_back(message);
                             return true;
                         });

    client.start(0);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);

    // CONNACK 一个字节一个字节地喂
    const ByteVec connack = connack_bytes(0x00);
    for (size_t i = 0; i < connack.size(); ++i) {
        transport.feed(&connack[i], 1);
        client.pump(0);
        if (i + 1 < connack.size()) CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);
    }
    CHECK_EQ(client.state(), mqttc::State::Connected);

    // QoS1 的 PUBLISH 也逐字节喂
    transport.clear_written();
    const ByteVec publish = publish_in("a/b", "hi", 1, 7);
    for (size_t i = 0; i < publish.size(); ++i) {
        transport.feed(&publish[i], 1);
        client.pump(0);
    }

    REQUIRE(messages.size() == 1);
    CHECK_EQ(messages[0].topic, std::string("a/b"));
    CHECK_EQ(messages[0].payload, (ByteVec{'h', 'i'}));
    CHECK_EQ(messages[0].qos, mqttc::QoS::AtLeastOnce);
    CHECK_EQ(client.stats().received, static_cast<uint64_t>(1));
    CHECK_EQ(client.stats().protocol_errors, static_cast<uint64_t>(0));

    auto acks = collect<mqttc::PubAck>(transport);
    REQUIRE(acks.size() == 1);
    CHECK_EQ(acks[0].packet_id, 7);

    // 一次给一堆字节（读缓冲一次能读走多个报文）也不能错
    transport.feed(publish_in("a", "1", 0, 0));
    transport.feed(publish_in("a", "2", 0, 0));
    transport.feed(pingresp_bytes());
    client.pump(1);
    CHECK_EQ(messages.size(), static_cast<size_t>(3));
    CHECK_EQ(client.stats().received, static_cast<uint64_t>(3));
}

TEST(client_drops_message_when_handler_overloaded) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options(), [](const mqttc::Message&) { return false; });
    connect_now(client, transport, 0);
    transport.clear_written();

    transport.feed(publish_in("a", "x", 1, 9));
    client.pump(0);

    CHECK_EQ(client.stats().received, static_cast<uint64_t>(1));
    CHECK_EQ(client.stats().dropped, static_cast<uint64_t>(1));
    // 丢了也要回 PUBACK，否则 broker 会一直重发
    auto acks = collect<mqttc::PubAck>(transport);
    REQUIRE(acks.size() == 1);
    CHECK_EQ(acks[0].packet_id, 9);
}

TEST(client_publish_and_subscribe_while_disconnected) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());

    // 还没连上
    CHECK_EQ(client.publish("a", ByteVec{'x'}, mqttc::QoS::AtMostOnce), mqttc::Error::Closed);
    CHECK_EQ(client.subscribe("a/+"), mqttc::Error::None);  // 先登记
    CHECK_EQ(client.subscription_count(), static_cast<size_t>(1));
    CHECK_EQ(client.subscribe("a/#/b"), mqttc::Error::ProtocolError);  // 过滤器非法（'#' 不在最后一层）
    CHECK_EQ(client.unsubscribe("a/+"), mqttc::Error::None);
    CHECK_EQ(client.subscription_count(), static_cast<size_t>(0));

    client.start(0);
    transport.clear_written();
    transport.feed(connack_bytes(0x00));
    client.pump(0);

    CHECK_EQ(collect<mqttc::Subscribe>(transport).size(), static_cast<size_t>(0));  // 已退订
    CHECK_EQ(client.subscribe("b/#"), mqttc::Error::None);
    CHECK_EQ(collect<mqttc::Subscribe>(transport).size(), static_cast<size_t>(1));
}

TEST(client_protocol_error_reconnects) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    transport.feed(ByteVec{0x00, 0x00});  // 保留报文类型
    client.pump(0);

    CHECK_EQ(client.stats().protocol_errors, static_cast<uint64_t>(1));
    CHECK_EQ(client.state(), mqttc::State::Backoff);
}

TEST(client_stop_sends_disconnect) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, default_options());
    connect_now(client, transport, 0);

    CHECK_EQ(client.stop(), mqttc::Error::None);
    CHECK_EQ(client.state(), mqttc::State::Idle);
    CHECK_EQ(collect<mqttc::Disconnect>(transport).size(), static_cast<size_t>(1));
    CHECK(!transport.connected());

    // 停掉以后不再自动重连
    transport.clear_written();
    client.pump(100000);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));
    CHECK_EQ(collect<mqttc::Connect>(transport).size(), static_cast<size_t>(0));
}
