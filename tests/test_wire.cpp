#include <string>

#include "mqttc/wire.hpp"
#include "test.hpp"

using mqttc::Error;
using mqttc::wire::ByteVec;
using mqttc::wire::Reader;
using mqttc::wire::Writer;

namespace {

void check_varint(uint32_t value, const ByteVec& expected) {
    ByteVec buffer;
    CHECK(mqttc::wire::append_varint(buffer, value));
    CHECK_EQ(buffer.size(), expected.size());
    for (size_t i = 0; i < buffer.size() && i < expected.size(); ++i) {
        CHECK_EQ(buffer[i], expected[i]);
    }

    Reader reader(buffer.data(), buffer.size());
    auto decoded = reader.varint();
    REQUIRE(decoded.ok());
    CHECK_EQ(decoded.value(), value);
    CHECK(reader.empty());
}

void check_varint_error(const ByteVec& buffer, Error expected) {
    Reader reader(buffer.data(), buffer.size());
    auto decoded = reader.varint();
    EXPECT_ERROR(decoded, expected);
}

void check_string_roundtrip(const std::string& text) {
    ByteVec buffer;
    Writer writer(buffer);
    writer.string(text);

    Reader reader(buffer.data(), buffer.size());
    auto decoded = reader.string();
    REQUIRE(decoded.ok());
    CHECK_EQ(std::string(decoded.value()), text);
    CHECK(reader.empty());
}

void check_utf8(const std::string& text, bool expected) {
    if (mqttc::wire::valid_utf8(text) != expected) {
        t::report(__FILE__, __LINE__, std::string("valid_utf8 mismatch for ") + text);
    }
}

void check_string_error(const ByteVec& buffer, Error expected) {
    Reader reader(buffer.data(), buffer.size());
    auto decoded = reader.string();
    EXPECT_ERROR(decoded, expected);
}

} // namespace

TEST(wire_u16_is_big_endian) {
    ByteVec buffer;
    Writer writer(buffer);
    writer.u16(0x1234);
    writer.u16(0x00FF);

    CHECK_EQ(buffer.size(), static_cast<size_t>(4));
    CHECK_EQ(buffer[0], 0x12);
    CHECK_EQ(buffer[1], 0x34);
    CHECK_EQ(buffer[2], 0x00);
    CHECK_EQ(buffer[3], 0xFF);

    Reader reader(buffer.data(), buffer.size());
    auto first = reader.u16();
    REQUIRE(first.ok());
    CHECK_EQ(first.value(), 0x1234);
    auto second = reader.u16();
    REQUIRE(second.ok());
    CHECK_EQ(second.value(), 0x00FF);
    CHECK(reader.empty());
}

TEST(wire_varint_boundaries) {
    check_varint(0, ByteVec{0x00});
    check_varint(127, ByteVec{0x7F});
    check_varint(128, ByteVec{0x80, 0x01});
    check_varint(16383, ByteVec{0xFF, 0x7F});
    check_varint(16384, ByteVec{0x80, 0x80, 0x01});
    check_varint(2097151, ByteVec{0xFF, 0xFF, 0x7F});
    check_varint(2097152, ByteVec{0x80, 0x80, 0x80, 0x01});
    check_varint(268435455, ByteVec{0xFF, 0xFF, 0xFF, 0x7F});
}

TEST(wire_varint_malformed) {
    // 只有续字节：还要等更多数据
    check_varint_error(ByteVec{0x80}, Error::WouldBlock);
    check_varint_error(ByteVec{0x80, 0x80}, Error::WouldBlock);

    // 第五个字节：超长
    check_varint_error(ByteVec{0x80, 0x80, 0x80, 0x80, 0x00}, Error::ProtocolError);
    check_varint_error(ByteVec{0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, Error::ProtocolError);
}

TEST(wire_varint_accepts_non_minimal_encodings) {
    // MQTT 3.1.1 没有要求 Remaining Length 用最短编码（"必须用最少字节"是 5.0 的规定），
    // 所以解码器接受 0x80 0x00 这类写法；编码器则永远输出最短形式。
    // 因此"解码后再编码必须逐字节相同"不是本层能承诺的——差异是 Linux 上的
    // libFuzzer 先撞出来的（输入 0xC0 0x80 0x00）。
    const ByteVec two_byte{0x80, 0x00};
    Reader reader(two_byte.data(), two_byte.size());
    auto zero = reader.varint();
    REQUIRE(zero.ok());
    CHECK_EQ(zero.value(), static_cast<uint32_t>(0));
    CHECK(reader.empty());

    const ByteVec three_byte{0x81, 0x80, 0x00};
    Reader padded(three_byte.data(), three_byte.size());
    auto one = padded.varint();
    REQUIRE(one.ok());
    CHECK_EQ(one.value(), static_cast<uint32_t>(1));
    CHECK(padded.empty());

    ByteVec buffer;
    CHECK(mqttc::wire::append_varint(buffer, one.value()));
    CHECK_EQ(buffer, ByteVec{0x01});
}

TEST(wire_varint_encode_rejects_oversize) {
    ByteVec buffer;
    CHECK(!mqttc::wire::append_varint(buffer, 268435456));  // 上限 + 1
    CHECK(buffer.empty());
    CHECK(mqttc::wire::append_varint(buffer, 268435455));
}

TEST(wire_reader_truncated) {
    const ByteVec none;
    Reader empty(none.data(), none.size());
    auto byte = empty.u8();
    EXPECT_ERROR(byte, Error::WouldBlock);

    const ByteVec one{0x12};
    Reader half(one.data(), one.size());
    auto word = half.u16();
    EXPECT_ERROR(word, Error::WouldBlock);
}

TEST(wire_string_roundtrip) {
    check_string_roundtrip("");
    check_string_roundtrip("mqtt");
    check_string_roundtrip("\xE4\xB8\xAD\xE6\x96\x87");  // "中文"
    check_string_roundtrip("\xF0\x9F\x98\x80");          // U+1F600
}

TEST(wire_string_errors) {
    // 长度前缀声明 5 字节，实际只有 2 字节
    check_string_error(ByteVec{0x00, 0x05, 0x61, 0x62}, Error::WouldBlock);
    // 只有长的低字节
    check_string_error(ByteVec{0x05}, Error::WouldBlock);

    // 声明 1 字节，内容 U+0000
    check_string_error(ByteVec{0x00, 0x01, 0x00}, Error::ProtocolError);
    // 过长编码
    check_string_error(ByteVec{0x00, 0x02, 0xC0, 0xAF}, Error::ProtocolError);
    // 代理对
    check_string_error(ByteVec{0x00, 0x03, 0xED, 0xA0, 0x80}, Error::ProtocolError);
    // 孤立续字节
    check_string_error(ByteVec{0x00, 0x01, 0x80}, Error::ProtocolError);
    // 截断的三字节序列
    check_string_error(ByteVec{0x00, 0x02, 0xE4, 0xB8}, Error::ProtocolError);
}

TEST(wire_valid_utf8_table) {
    check_utf8("", true);
    check_utf8("abc", true);
    check_utf8("\x7F", true);
    check_utf8("\xC2\x80", true);          // U+0080 最短编码
    check_utf8("\xE4\xB8\xAD", true);      // U+4E2D
    check_utf8("\xEF\xBF\xBF", true);      // U+FFFF
    check_utf8("\xF0\x90\x80\x80", true);  // U+10000
    check_utf8("\xF4\x8F\xBF\xBF", true);  // U+10FFFF

    check_utf8(std::string("\x00", 1), false);  // NUL
    check_utf8(std::string("a\x00b", 3), false);
    check_utf8("\xC0\xAF", false);                 // 过长编码
    check_utf8("\xE0\x80\xAF", false);             // 过长编码
    check_utf8("\xED\xA0\x80", false);             // 代理对
    check_utf8("\xF5\x80\x80\x80", false);         // 超过 U+10FFFF
    check_utf8("\xF4\x90\x80\x80", false);         // 超过 U+10FFFF
    check_utf8("\x80", false);                     // 孤立续字节
    check_utf8("\xE4\xB8", false);                 // 截断
    check_utf8("\xC2", false);                     // 截断
}

TEST(wire_reader_bytes) {
    const ByteVec source{0x01, 0x02, 0x03};
    Reader reader(source.data(), source.size());

    auto two = reader.bytes(2);
    REQUIRE(two.ok());
    CHECK_EQ(two.value(), (ByteVec{0x01, 0x02}));
    CHECK_EQ(reader.remaining(), static_cast<size_t>(1));

    auto too_many = reader.bytes(2);
    EXPECT_ERROR(too_many, Error::WouldBlock);

    auto zero = reader.bytes(0);
    REQUIRE(zero.ok());
    CHECK(zero.value().empty());
}
