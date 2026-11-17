#include <string>
#include <variant>

#include "mqttc/packet.hpp"
#include "test.hpp"

namespace {

using mqttc::ByteVec;
using mqttc::Error;
using mqttc::Packet;

std::string to_hex(uint8_t byte) {
    const char* digits = "0123456789ABCDEF";
    std::string text = "00";
    text[0] = digits[byte >> 4];
    text[1] = digits[byte & 0x0F];
    return text;
}

void expect_bytes(const ByteVec& actual, std::initializer_list<uint8_t> expected) {
    if (actual.size() != expected.size()) {
        t::report(__FILE__, __LINE__, "size mismatch: got " + std::to_string(actual.size()) +
                                          ", want " + std::to_string(expected.size()));
        return;
    }
    size_t index = 0;
    for (uint8_t byte : expected) {
        if (actual[index] != byte) {
            t::report(__FILE__, __LINE__, "byte " + std::to_string(index) + " mismatch: got 0x" +
                                              to_hex(actual[index]) + ", want 0x" + to_hex(byte));
        }
        ++index;
    }
}

mqttc::Result<Packet> decode_list(std::initializer_list<uint8_t> bytes) {
    const ByteVec buffer(bytes);
    return mqttc::decode(buffer);
}

void expect_decode_error(std::initializer_list<uint8_t> bytes, Error expected, const char* what) {
    auto result = decode_list(bytes);
    t::expect_error(result, expected, what, __FILE__, __LINE__);
}

void check_publish_length_boundary(size_t payload_size, std::initializer_list<uint8_t> length_bytes) {
    mqttc::Publish packet;
    packet.topic = "t";
    packet.payload.assign(payload_size, 0xAB);

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    const ByteVec& bytes = encoded.value();
    REQUIRE(bytes.size() >= 1 + length_bytes.size());

    CHECK_EQ(bytes[0], 0x30);
    size_t index = 1;
    for (uint8_t expected : length_bytes) {
        CHECK_EQ(bytes[index], expected);
        ++index;
    }
    CHECK_EQ(bytes.size(), 1 + length_bytes.size() + 3 + payload_size);

    auto decoded = mqttc::decode(bytes);
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Publish>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->payload.size(), payload_size);
}

} // namespace

// ---------------------------------------------------------------- CONNECT

TEST(packet_connect_minimal_bytes) {
    mqttc::Connect packet;
    packet.client_id = "mqtt";
    packet.clean_session = true;
    packet.keepalive = 60;

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x10, 0x10, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x02,
                                   0x00, 0x3C, 0x00, 0x04, 0x6D, 0x71, 0x74, 0x74});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Connect>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->client_id, std::string("mqtt"));
    CHECK(got->clean_session);
    CHECK_EQ(got->keepalive, 60);
    CHECK(!got->will.has_value());
    CHECK(!got->username.has_value());
    CHECK(!got->password.has_value());
}

TEST(packet_connect_full_roundtrip) {
    mqttc::Connect packet;
    packet.client_id = "cid";
    packet.clean_session = true;
    packet.keepalive = 30;

    mqttc::Will will;
    will.topic = "will/topic";
    will.payload = ByteVec{'b', 'y', 'e'};
    will.qos = 1;
    will.retain = true;
    packet.will = will;
    packet.username = "user";
    packet.password = "pass";

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    CHECK_EQ(encoded.value().size(), static_cast<size_t>(2 + 44));
    CHECK_EQ(encoded.value()[1], 0x2C);
    // 用户名|密码|will retain|will qos1|will flag|clean session
    CHECK_EQ(encoded.value()[9], 0xEE);

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Connect>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->client_id, std::string("cid"));
    CHECK(got->clean_session);
    CHECK_EQ(got->keepalive, 30);
    REQUIRE(got->will.has_value());
    CHECK_EQ(got->will->topic, std::string("will/topic"));
    CHECK_EQ(got->will->payload, will.payload);
    CHECK_EQ(got->will->qos, 1);
    CHECK(got->will->retain);
    REQUIRE(got->username.has_value());
    CHECK_EQ(*got->username, std::string("user"));
    REQUIRE(got->password.has_value());
    CHECK_EQ(*got->password, std::string("pass"));
}

TEST(packet_connect_empty_client_id_with_clean_session) {
    mqttc::Connect packet;
    packet.client_id = "";
    packet.clean_session = true;
    packet.keepalive = 0;

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x10, 0x0C, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x02,
                                   0x00, 0x00, 0x00, 0x00});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Connect>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK(got->client_id.empty());
}

TEST(packet_connect_encode_rejects_bad_input) {
    mqttc::Connect packet;
    packet.client_id = "cid";

    packet.clean_session = false;
    packet.client_id = "";
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 空 client id 必须 clean session

    packet.client_id = "cid";
    packet.password = "p";
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 只有密码位

    packet.password.reset();
    mqttc::Will will;
    will.topic = "will";
    will.qos = 2;
    packet.will = will;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 不支持 QoS2

    will.qos = 1;
    will.topic = "will/#";
    packet.will = will;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // will 主题名不能带通配符

    packet.will.reset();
    packet.client_id = std::string("a\x00b", 3);
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 非法 UTF-8
}

TEST(packet_connect_decode_rejects_bad_input) {
    // 协议名错
    expect_decode_error({0x10, 0x0C, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x55, 0x04, 0x02, 0x00, 0x00,
                         0x00, 0x00},
                        Error::ProtocolError, "bad protocol name");
    // 协议级别不是 4
    expect_decode_error({0x10, 0x0C, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x00,
                         0x00, 0x00},
                        Error::ProtocolError, "bad protocol level");
    // 保留位为 1
    expect_decode_error({0x10, 0x0C, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x03, 0x00, 0x00,
                         0x00, 0x00},
                        Error::ProtocolError, "reserved bit set");
    // 密码位单独置位
    expect_decode_error({0x10, 0x0C, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x42, 0x00, 0x00,
                         0x00, 0x00},
                        Error::ProtocolError, "password without username");
    // 固定头标志位非 0
    expect_decode_error({0x11, 0x0C, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x00,
                         0x00, 0x00},
                        Error::ProtocolError, "connect flags in fixed header");
    // 声明长度短于实际内容
    expect_decode_error({0x10, 0x02, 0x00, 0x04}, Error::ProtocolError, "truncated body");
    // 长度不够读数据
    expect_decode_error({0x10, 0x0C, 0x00, 0x04, 0x4D}, Error::WouldBlock, "incomplete packet");
}

// ---------------------------------------------------------------- CONNACK

TEST(packet_connack_roundtrip) {
    mqttc::ConnAck packet;
    packet.session_present = true;
    packet.return_code = 0;

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x20, 0x02, 0x01, 0x00});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::ConnAck>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK(got->session_present);
    CHECK_EQ(got->return_code, 0x00);
}

TEST(packet_connack_rejects_bad_input) {
    mqttc::ConnAck packet;
    packet.return_code = 6;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);

    expect_decode_error({0x20, 0x02, 0x00, 0x06}, Error::ProtocolError, "return code 6");
    expect_decode_error({0x20, 0x02, 0x02, 0x00}, Error::ProtocolError, "reserved ack flags");
    expect_decode_error({0x20, 0x03, 0x00, 0x00, 0x00}, Error::ProtocolError, "extra body byte");
    expect_decode_error({0x20, 0x01, 0x00}, Error::ProtocolError, "short body");
    expect_decode_error({0x21, 0x02, 0x00, 0x00}, Error::ProtocolError, "fixed header flags");
}

// ---------------------------------------------------------------- PUBLISH

TEST(packet_publish_qos0_bytes) {
    mqttc::Publish packet;
    packet.topic = "t";

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x30, 0x03, 0x00, 0x01, 0x74});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Publish>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->topic, std::string("t"));
    CHECK_EQ(got->qos, 0);
    CHECK_EQ(got->packet_id, 0);
    CHECK(got->payload.empty());
    CHECK(!got->dup);
    CHECK(!got->retain);
}

TEST(packet_publish_qos1_bytes) {
    mqttc::Publish packet;
    packet.topic = "a/b";
    packet.payload = ByteVec{'h', 'i'};
    packet.qos = 1;
    packet.packet_id = 10;

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x32, 0x09, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x0A, 0x68,
                                   0x69});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Publish>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->qos, 1);
    CHECK_EQ(got->packet_id, 10);
    CHECK_EQ(got->payload, (ByteVec{'h', 'i'}));
}

TEST(packet_publish_dup_retain_bits) {
    mqttc::Publish packet;
    packet.topic = "a";
    packet.qos = 1;
    packet.packet_id = 7;
    packet.dup = true;
    packet.retain = true;
    packet.payload = ByteVec{0x00, 0xFF, 0x00};  // 二进制负载，含 0x00

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    CHECK_EQ(encoded.value()[0], 0x3B);  // DUP=1 QoS=1 RETAIN=1

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Publish>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK(got->dup);
    CHECK(got->retain);
    CHECK_EQ(got->payload, (ByteVec{0x00, 0xFF, 0x00}));
}

TEST(packet_publish_remaining_length_boundaries) {
    check_publish_length_boundary(0, {0x03});
    check_publish_length_boundary(124, {0x7F});          // remaining = 127
    check_publish_length_boundary(125, {0x80, 0x01});    // remaining = 128
    check_publish_length_boundary(16380, {0xFF, 0x7F});  // remaining = 16383
    check_publish_length_boundary(16381, {0x80, 0x80, 0x01});  // remaining = 16384
}

TEST(packet_publish_encode_rejects_bad_input) {
    mqttc::Publish packet;
    packet.topic = "a";

    packet.qos = 2;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 不支持 QoS2

    packet.qos = 1;
    packet.packet_id = 0;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // QoS>0 必须有 packet id

    packet.packet_id = 1;
    packet.qos = 0;
    packet.dup = true;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // QoS0 不能置 DUP

    packet.dup = false;
    packet.topic = "a/+";
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 主题名不能带通配符

    packet.topic = std::string(65536, 'a');
    EXPECT_ERROR(mqttc::encode(packet), Error::Oversize);
}

TEST(packet_publish_decode_rejects_bad_input) {
    // QoS 位 = 3
    expect_decode_error({0x36, 0x05, 0x00, 0x01, 0x74, 0x00, 0x01}, Error::ProtocolError, "qos 3");
    // QoS0 却置 DUP
    expect_decode_error({0x38, 0x03, 0x00, 0x01, 0x74}, Error::ProtocolError, "dup with qos0");
    // QoS1 的 packet id 为 0
    expect_decode_error({0x32, 0x05, 0x00, 0x01, 0x74, 0x00, 0x00}, Error::ProtocolError,
                        "zero packet id");
    // 主题名带通配符
    expect_decode_error({0x30, 0x04, 0x00, 0x02, 0x61, 0x2B}, Error::ProtocolError, "wildcard topic");
    // 空主题名
    expect_decode_error({0x30, 0x02, 0x00, 0x00}, Error::ProtocolError, "empty topic");
    // 声明长度超出可用数据
    expect_decode_error({0x30, 0x05, 0x00, 0x01, 0x74}, Error::WouldBlock, "truncated");
}

// ---------------------------------------------------------------- PUBACK

TEST(packet_puback_roundtrip) {
    mqttc::PubAck packet;
    packet.packet_id = 1;

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x40, 0x02, 0x00, 0x01});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::PubAck>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->packet_id, 1);
}

TEST(packet_puback_rejects_bad_input) {
    mqttc::PubAck packet;
    packet.packet_id = 0;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);

    expect_decode_error({0x40, 0x02, 0x00, 0x00}, Error::ProtocolError, "zero packet id");
    expect_decode_error({0x41, 0x02, 0x00, 0x01}, Error::ProtocolError, "fixed header flags");
    expect_decode_error({0x40, 0x01, 0x00}, Error::ProtocolError, "short body");
}

// ---------------------------------------------------------------- SUBSCRIBE / SUBACK

TEST(packet_subscribe_roundtrip) {
    mqttc::Subscribe packet;
    packet.packet_id = 1;
    packet.entries.push_back(mqttc::Subscription{"a/#", 1});

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x82, 0x08, 0x00, 0x01, 0x00, 0x03, 0x61, 0x2F, 0x23, 0x01});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Subscribe>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->packet_id, 1);
    REQUIRE(got->entries.size() == 1);
    CHECK_EQ(got->entries[0].filter, std::string("a/#"));
    CHECK_EQ(got->entries[0].qos, 1);
}

TEST(packet_subscribe_multiple_filters) {
    mqttc::Subscribe packet;
    packet.packet_id = 0x0102;
    packet.entries.push_back(mqttc::Subscription{"a/+", 0});
    packet.entries.push_back(mqttc::Subscription{"b/#", 1});

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Subscribe>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->packet_id, 0x0102);
    REQUIRE(got->entries.size() == 2);
    CHECK_EQ(got->entries[1].filter, std::string("b/#"));
    CHECK_EQ(got->entries[1].qos, 1);
}

TEST(packet_subscribe_rejects_bad_input) {
    mqttc::Subscribe packet;
    packet.packet_id = 1;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 没有过滤器

    packet.entries.push_back(mqttc::Subscription{"a/#", 1});
    packet.packet_id = 0;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // packet id 为 0

    packet.packet_id = 1;
    packet.entries[0].qos = 2;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 不支持 QoS2

    packet.entries[0].qos = 1;
    packet.entries[0].filter = "a/#/b";
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 非法过滤器

    // 固定头标志位必须是 0b0010
    expect_decode_error({0x80, 0x08, 0x00, 0x01, 0x00, 0x03, 0x61, 0x2F, 0x23, 0x01},
                        Error::ProtocolError, "wrong subscribe flags");
    // 空负载
    expect_decode_error({0x82, 0x02, 0x00, 0x01}, Error::ProtocolError, "empty payload");
    // 请求 QoS 3
    expect_decode_error({0x82, 0x08, 0x00, 0x01, 0x00, 0x03, 0x61, 0x2F, 0x23, 0x03},
                        Error::ProtocolError, "requested qos 3");
    // 请求 QoS 2（协议合法，但本库不做 QoS2，直接判协议错误）
    expect_decode_error({0x82, 0x08, 0x00, 0x01, 0x00, 0x03, 0x61, 0x2F, 0x23, 0x02},
                        Error::ProtocolError, "requested qos 2");
    // 过滤器非法
    expect_decode_error({0x82, 0x08, 0x00, 0x01, 0x00, 0x03, 0x61, 0x23, 0x62, 0x01},
                        Error::ProtocolError, "bad filter");
}

TEST(packet_suback_roundtrip) {
    mqttc::SubAck packet;
    packet.packet_id = 1;
    packet.return_codes.push_back(0x00);

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0x90, 0x03, 0x00, 0x01, 0x00});

    packet.return_codes.push_back(0x80);
    packet.return_codes.push_back(0x01);
    auto encoded_multi = mqttc::encode(packet);
    REQUIRE(encoded_multi.ok());

    auto decoded = mqttc::decode(encoded_multi.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::SubAck>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->packet_id, 1);
    CHECK_EQ(got->return_codes, (std::vector<uint8_t>{0x00, 0x80, 0x01}));
}

TEST(packet_suback_rejects_bad_input) {
    mqttc::SubAck packet;
    packet.packet_id = 1;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 没有返回码

    packet.return_codes.push_back(0x03);
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 非法返回码

    expect_decode_error({0x90, 0x02, 0x00, 0x01}, Error::ProtocolError, "empty payload");
    expect_decode_error({0x90, 0x03, 0x00, 0x01, 0x03}, Error::ProtocolError, "bad return code");
    expect_decode_error({0x90, 0x03, 0x00, 0x01, 0x02}, Error::ProtocolError,
                        "granted qos 2 unsupported");
    expect_decode_error({0x91, 0x03, 0x00, 0x01, 0x00}, Error::ProtocolError, "fixed header flags");
}

// ---------------------------------------------------------------- UNSUBSCRIBE / UNSUBACK

TEST(packet_unsubscribe_roundtrip) {
    mqttc::Unsubscribe packet;
    packet.packet_id = 1;
    packet.filters.push_back("a/#");

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0xA2, 0x07, 0x00, 0x01, 0x00, 0x03, 0x61, 0x2F, 0x23});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::Unsubscribe>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->packet_id, 1);
    CHECK_EQ(got->filters, (std::vector<std::string>{"a/#"}));
}

TEST(packet_unsubscribe_rejects_bad_input) {
    mqttc::Unsubscribe packet;
    packet.packet_id = 1;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 没有过滤器

    packet.filters.push_back("a/#/b");
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);  // 非法过滤器

    packet.filters[0] = "a/#";
    packet.packet_id = 0;
    EXPECT_ERROR(mqttc::encode(packet), Error::ProtocolError);

    expect_decode_error({0xA2, 0x02, 0x00, 0x01}, Error::ProtocolError, "empty payload");
    expect_decode_error({0xA0, 0x07, 0x00, 0x01, 0x00, 0x03, 0x61, 0x2F, 0x23},
                        Error::ProtocolError, "wrong unsubscribe flags");
}

TEST(packet_unsuback_roundtrip) {
    mqttc::UnsubAck packet;
    packet.packet_id = 1;

    auto encoded = mqttc::encode(packet);
    REQUIRE(encoded.ok());
    expect_bytes(encoded.value(), {0xB0, 0x02, 0x00, 0x01});

    auto decoded = mqttc::decode(encoded.value());
    REQUIRE(decoded.ok());
    const auto* got = std::get_if<mqttc::UnsubAck>(&decoded.value());
    REQUIRE(got != nullptr);
    CHECK_EQ(got->packet_id, 1);

    expect_decode_error({0xB0, 0x02, 0x00, 0x00}, Error::ProtocolError, "zero packet id");
}

// ---------------------------------------------------------------- 无负载报文

TEST(packet_ping_and_disconnect) {
    auto ping_req = mqttc::encode(mqttc::PingReq{});
    REQUIRE(ping_req.ok());
    expect_bytes(ping_req.value(), {0xC0, 0x00});
    auto decoded_req = mqttc::decode(ping_req.value());
    REQUIRE(decoded_req.ok());
    CHECK(std::get_if<mqttc::PingReq>(&decoded_req.value()) != nullptr);

    auto ping_resp = mqttc::encode(mqttc::PingResp{});
    REQUIRE(ping_resp.ok());
    expect_bytes(ping_resp.value(), {0xD0, 0x00});
    auto decoded_resp = mqttc::decode(ping_resp.value());
    REQUIRE(decoded_resp.ok());
    CHECK(std::get_if<mqttc::PingResp>(&decoded_resp.value()) != nullptr);

    auto disconnect = mqttc::encode(mqttc::Disconnect{});
    REQUIRE(disconnect.ok());
    expect_bytes(disconnect.value(), {0xE0, 0x00});
    auto decoded_disconnect = mqttc::decode(disconnect.value());
    REQUIRE(decoded_disconnect.ok());
    CHECK(std::get_if<mqttc::Disconnect>(&decoded_disconnect.value()) != nullptr);

    // 剩余长度必须是 0
    expect_decode_error({0xC0, 0x01, 0x00}, Error::ProtocolError, "pingreq with payload");
    expect_decode_error({0xC1, 0x00}, Error::ProtocolError, "pingreq flags");
    expect_decode_error({0xE1, 0x00}, Error::ProtocolError, "disconnect flags");
}

// ---------------------------------------------------------------- 固定头与分帧

TEST(packet_decode_rejects_reserved_types) {
    expect_decode_error({0x00, 0x00}, Error::ProtocolError, "type 0");
    expect_decode_error({0xF0, 0x00}, Error::ProtocolError, "type 15");
}

TEST(packet_decode_needs_more_data) {
    expect_decode_error({}, Error::WouldBlock, "empty buffer");
    expect_decode_error({0xC0}, Error::WouldBlock, "only type byte");
    expect_decode_error({0x30, 0x80}, Error::WouldBlock, "half varint");
    expect_decode_error({0x30, 0x80, 0x80, 0x80, 0x80, 0x00}, Error::ProtocolError,
                        "varint too long");
}

TEST(packet_decode_accepts_non_minimal_remaining_length) {
    // 0xC0 0x80 0x00：PINGREQ，Remaining Length 用两个字节写 0。
    // 3.1.1 不要求最短编码（5.0 才要求），所以这里接受；编码器输出最短的 C0 00。
    // 这个输入是 Linux 上 libFuzzer 撞出来的（当时失败的是 fuzzer 自己的不变量）。
    const ByteVec raw{0xC0, 0x80, 0x00};
    auto size = mqttc::peek_packet_size(raw.data(), raw.size());
    REQUIRE(size.ok());
    CHECK_EQ(size.value(), static_cast<size_t>(3));

    auto decoded = decode_list({0xC0, 0x80, 0x00});
    REQUIRE(decoded.ok());
    CHECK(std::holds_alternative<mqttc::PingReq>(decoded.value()));

    auto reencoded = std::visit([](const auto& value) { return mqttc::encode(value); },
                                decoded.value());
    REQUIRE(reencoded.ok());
    expect_bytes(reencoded.value(), {0xC0, 0x00});
}

TEST(packet_decode_rejects_trailing_bytes) {
    expect_decode_error({0xC0, 0x00, 0x00}, Error::ProtocolError, "one byte too many");
    expect_decode_error({0x40, 0x02, 0x00, 0x01, 0xFF}, Error::ProtocolError, "puback with tail");
}

TEST(packet_peek_packet_size) {
    const ByteVec publish{0x30, 0x03, 0x00, 0x01, 0x74};
    auto size = mqttc::peek_packet_size(publish.data(), 2);  // 只看固定头即可
    REQUIRE(size.ok());
    CHECK_EQ(size.value(), static_cast<size_t>(5));

    const ByteVec incomplete{0x30, 0x80};
    auto blocked = mqttc::peek_packet_size(incomplete.data(), incomplete.size());
    EXPECT_ERROR(blocked, Error::WouldBlock);

    const ByteVec single{0xC0};
    auto one_byte = mqttc::peek_packet_size(single.data(), single.size());
    EXPECT_ERROR(one_byte, Error::WouldBlock);

    const ByteVec malformed{0xC0, 0x80, 0x80, 0x80, 0x80, 0x00};
    auto bad = mqttc::peek_packet_size(malformed.data(), malformed.size());
    EXPECT_ERROR(bad, Error::ProtocolError);

    const ByteVec big{0x30, 0xFF, 0xFF, 0xFF, 0x7F};
    auto big_size = mqttc::peek_packet_size(big.data(), big.size());
    REQUIRE(big_size.ok());
    CHECK_EQ(big_size.value(), static_cast<size_t>(5 + 268435455));
}
