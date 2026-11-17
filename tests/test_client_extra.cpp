// 状态机的补充覆盖：之前没测到的边界与资源上界。
// 与 test_client.cpp 分开放，那边是原有行为，这边是"补洞"。

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/log.hpp"
#include "mqttc/packet.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

#include "support.hpp"
#include "test.hpp"

namespace {

// 一个"声明了指定长度、后面不给够字节"的 PUBLISH 头
mqttc::ByteVec publish_header(uint32_t remaining_length) {
    mqttc::ByteVec bytes;
    bytes.push_back(0x30);  // PUBLISH, QoS 0
    mqttc::wire::append_varint(bytes, remaining_length);
    return bytes;
}

} // namespace

TEST(client_unsuback_releases_packet_id) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with());
    support::connect(transport, client);

    // packet id 一共 65535 个。每发一次 UNSUBSCRIBE，都要在收到 UNSUBACK 后把它还回去；
    // 不还的话第 65536 次就分配不出 id 了——最后那次断言就是来抓这个泄漏的。
    for (int i = 0; i < 65535; ++i) {
        transport.clear_written();
        CHECK_EQ(client.unsubscribe("a/b"), mqttc::Error::None);
        transport.feed(support::unsuback(support::last_request_packet_id(transport.written())));
        client.pump(0);
    }

    transport.clear_written();
    CHECK_EQ(client.unsubscribe("a/b"), mqttc::Error::None);
}

TEST(client_stray_unsuback_does_not_free_someone_elses_id) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with());
    support::connect(transport, client);

    // 一条 QoS1 消息拿走 1 号，还在等 PUBACK
    CHECK_EQ(client.publish("a/b", mqttc::ByteVec{'x'}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(1));

    // 服务端回一个 id 对不上的 UNSUBACK：不能顺手把 1 号还回去
    transport.feed(support::unsuback(1));
    client.pump(0);
    CHECK_EQ(client.stats().unexpected_packets, static_cast<uint64_t>(1));
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(1));

    // 再发一条必须拿新的 id：复用 1 号就会和还在途的那条串在一起
    transport.clear_written();
    CHECK_EQ(client.publish("a/b", mqttc::ByteVec{'y'}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    CHECK_EQ(support::last_request_packet_id(transport.written()), static_cast<uint16_t>(2));
}

TEST(client_stray_suback_does_not_free_someone_elses_id) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with());
    support::connect(transport, client);

    CHECK_EQ(client.publish("a/b", mqttc::ByteVec{'x'}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);

    transport.feed(support::suback(1, {0x00}));
    client.pump(0);
    CHECK_EQ(client.stats().unexpected_packets, static_cast<uint64_t>(1));

    transport.clear_written();
    CHECK_EQ(client.publish("a/b", mqttc::ByteVec{'y'}, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    CHECK_EQ(support::last_request_packet_id(transport.written()), static_cast<uint16_t>(2));
}

TEST(client_publish_reports_busy_at_max_inflight) {
    mqttc::InMemoryTransport transport;
    mqttc::ClientOptions options = support::options_with();
    options.max_inflight = 3;
    mqttc::Client client(transport, options);
    support::connect(transport, client);

    const mqttc::ByteVec payload = {'x'};
    for (int i = 0; i < 3; ++i) {
        CHECK_EQ(client.publish("a/b", payload, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
    }
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(3));

    // 到顶了：不往内部无限堆，明确告诉调用方"忙"
    CHECK_EQ(client.publish("a/b", payload, mqttc::QoS::AtLeastOnce), mqttc::Error::Busy);

    // 一条 PUBACK 腾出位置，就又能发了
    transport.feed(support::puback(1));
    client.pump(0);
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(2));
    CHECK_EQ(client.publish("a/b", payload, mqttc::QoS::AtLeastOnce), mqttc::Error::None);
}

TEST(client_qos0_publish_is_not_limited_by_max_inflight) {
    mqttc::InMemoryTransport transport;
    mqttc::ClientOptions options = support::options_with();
    options.max_inflight = 1;
    mqttc::Client client(transport, options);
    support::connect(transport, client);

    const mqttc::ByteVec payload = {'x'};
    for (int i = 0; i < 10; ++i) {
        CHECK_EQ(client.publish("a/b", payload, mqttc::QoS::AtMostOnce), mqttc::Error::None);
    }
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(0));  // QoS0 不入在途表
}

TEST(client_connack_timeout_is_ten_seconds) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with());
    client.start(0);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);

    client.pump(9999);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);  // 还没到
    client.pump(10000);
    CHECK_EQ(client.state(), mqttc::State::Backoff);         // 到点判连接失败
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(1));
}

TEST(client_oversize_varint_is_protocol_error) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with());
    support::connect(transport, client);

    // 长度字段用了 5 个字节（上限 4 个）
    mqttc::ByteVec bytes;
    bytes.push_back(0x30);
    bytes.insert(bytes.end(), {0xFF, 0xFF, 0xFF, 0xFF, 0x7F});
    transport.feed(bytes);
    client.pump(0);

    CHECK(client.stats().protocol_errors >= 1);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
}

TEST(client_oversize_declared_packet_is_rejected) {
    mqttc::InMemoryTransport transport;
    mqttc::ClientOptions options = support::options_with();
    options.max_packet_bytes = 4096;  // 策略上限，测试里调小
    mqttc::Client client(transport, options);
    support::connect(transport, client);

    // 对端一开口就说"这个报文有 4096 字节"：加上固定头与长度字段已经超过上限，
    // 不用陪着它把内存吃下去
    transport.feed(publish_header(4096));
    client.pump(0);

    CHECK(client.stats().oversize_packets >= 1);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
}

TEST(client_input_buffer_is_bounded_by_policy) {
    mqttc::InMemoryTransport transport;
    mqttc::ClientOptions options = support::options_with();
    options.max_packet_bytes = 8192;
    mqttc::Client client(transport, options);
    support::connect(transport, client);

    // 声明 8000 字节（在策略上限内），然后一直喂但永远凑不满这个报文：
    // 接收缓冲涨到上限就必须自己断开，不能无限涨
    transport.feed(publish_header(8000));
    client.pump(0);
    CHECK_EQ(client.state(), mqttc::State::Connected);

    const mqttc::ByteVec chunk(4096, 0x00);
    for (int i = 0; i < 8 && client.state() == mqttc::State::Connected; ++i) {
        transport.feed(chunk);
        client.pump(0);
    }

    CHECK(client.stats().oversize_packets >= 1);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
}

TEST(client_session_present_is_accepted) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with());
    support::connect(transport, client, /*session_present=*/true);
    CHECK_EQ(client.state(), mqttc::State::Connected);
}

TEST(client_connect_carries_will_and_credentials) {
    mqttc::InMemoryTransport transport;

    mqttc::ClientOptions options = support::options_with();
    options.username = "user";
    options.password = "pass";
    options.keepalive = 30;

    mqttc::Will will;
    will.topic = "dev/offline";
    will.payload = {'b', 'y', 'e'};
    will.qos = 1;
    will.retain = true;
    options.will = will;

    mqttc::Client client(transport, options);
    client.start(0);

    auto decoded = mqttc::decode(transport.written());
    REQUIRE(decoded.ok());
    const auto* connect = std::get_if<mqttc::Connect>(&decoded.value());
    REQUIRE(connect != nullptr);

    CHECK_EQ(connect->client_id, std::string("tester"));
    CHECK_EQ(connect->keepalive, static_cast<uint16_t>(30));
    REQUIRE(connect->username.has_value());
    CHECK_EQ(*connect->username, std::string("user"));
    REQUIRE(connect->password.has_value());
    CHECK_EQ(*connect->password, std::string("pass"));
    REQUIRE(connect->will.has_value());
    CHECK_EQ(connect->will->topic, std::string("dev/offline"));
    CHECK_EQ(connect->will->payload.size(), static_cast<size_t>(3));
    CHECK_EQ(connect->will->qos, static_cast<uint8_t>(1));
    CHECK(connect->will->retain);
}

// ---- "对端先发最后一包、再关连接"的顺序问题 ----
// 真实 broker 拒绝连接时就是"发 CONNACK 错误码 + 关 socket"。以前 drain_input
// 一读到 Closed 就立刻重连，把刚读到的 CONNACK 丢在缓冲里没解析，于是永久失败
// 被当成网络抖动，一路重试到超时。这几个用例把这个顺序钉住。

namespace {

// 收集日志的行，用来断言"这次重连是哪个原因"
struct LogCapture {
    std::shared_ptr<std::vector<std::string>> lines = std::make_shared<std::vector<std::string>>();

    mqttc::Logger make_logger() {
        auto lines = this->lines;  // 共享所有权：客户端的 sink 一直往里追加
        return mqttc::Logger(
            [lines](mqttc::LogLevel, const std::string& text) { lines->push_back(text); },
            mqttc::LogLevel::Debug);
    }

    bool any(const std::string& needle) const {
        for (const std::string& line : *lines) {
            if (line.find(needle) != std::string::npos) return true;
        }
        return false;
    }
};

} // namespace

TEST(client_parses_a_connack_that_arrives_just_before_the_peer_closes) {
    LogCapture logs;
    mqttc::ClientOptions options = support::options_with();
    options.logger = logs.make_logger();

    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);

    client.start(0);
    transport.feed(support::connack(4));  // 凭据错
    transport.set_closed(true);           // 紧接着对端就关了

    client.pump(0);

    CHECK_EQ(client.state(), mqttc::State::Failed);
    CHECK(logs.any("connack_rejected"));
    CHECK(!logs.any("reason=peer_closed"));  // 不是按"网络抖动"处理
}

TEST(client_reconnects_as_peer_closed_when_the_peer_closes_after_a_good_connack) {
    LogCapture logs;
    mqttc::ClientOptions options = support::options_with();
    options.logger = logs.make_logger();

    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);

    client.start(0);
    transport.feed(support::connack(0x00));
    transport.set_closed(true);
    client.pump(0);

    // CONNACK 先被认下来（连上过），然后才因为对端关闭掉线
    CHECK(logs.any("connected"));
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK(logs.any("reason=peer_closed"));
}

TEST(client_delivers_a_publish_that_arrives_just_before_the_peer_closes) {
    std::vector<std::string> received;
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, support::options_with(), [&received](const mqttc::Message& m) {
        received.push_back(m.topic);
        return true;
    });

    support::connect(transport, client);

    mqttc::Publish publish;
    publish.topic = "a/b";
    publish.payload = {1, 2, 3};
    transport.feed(support::bytes_of(publish));
    transport.set_closed(true);  // 消息之后紧跟 FIN

    client.pump(0);

    CHECK_EQ(received.size(), static_cast<size_t>(1));  // 这一包不能因为 FIN 被丢掉
    CHECK_EQ(received[0], std::string("a/b"));
    CHECK_EQ(client.state(), mqttc::State::Backoff);
}
