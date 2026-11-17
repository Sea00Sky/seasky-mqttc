// 不依赖 libFuzzer 的"就地模糊"：用固定种子的伪随机字节 + 合法报文的变异，
// 反复砸 peek/decode/encode 与主题校验这几个纯函数。
//
// 目的不是覆盖率，而是**任何编译器、任何平台上都能跑**的一层下限：
// 不崩、不越界、不返回自相矛盾的结果。真正的 libFuzzer 目标在 fuzz/ 下，
// 只有 clang 且显式打开 MQTTC_BUILD_FUZZERS 才构建。

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "mqttc/packet.hpp"
#include "mqttc/topic.hpp"
#include "mqttc/wire.hpp"

#include "test.hpp"

namespace {

// xorshift64*：固定种子 => 每次都跑同一批输入，测试不会偶发
struct Rng {
    uint64_t state = 0x9E3779B97F4A7C15ull;

    uint32_t next() {
        uint64_t x = state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        state = x;
        return static_cast<uint32_t>((x * 0x2545F4914F6CDD1Dull) >> 32);
    }

    uint8_t byte() { return static_cast<uint8_t>(next() & 0xFF); }
    size_t below(size_t limit) { return limit == 0 ? 0 : next() % limit; }
};

// 一组合法报文，给"变异"用
std::vector<mqttc::ByteVec> valid_corpus() {
    std::vector<mqttc::ByteVec> corpus;

    mqttc::Connect connect;
    connect.client_id = "fuzz";
    connect.keepalive = 30;
    connect.username = "u";
    connect.password = "p";
    corpus.push_back(mqttc::encode(connect).value());

    mqttc::Publish publish;
    publish.topic = "a/b/c";
    publish.payload = {'p', 'a', 'y', 'l', 'o', 'a', 'd'};
    publish.qos = 1;
    publish.packet_id = 7;
    publish.retain = true;
    corpus.push_back(mqttc::encode(publish).value());

    mqttc::Subscribe subscribe;
    subscribe.packet_id = 9;
    subscribe.entries.push_back(mqttc::Subscription{"a/#", 1});
    subscribe.entries.push_back(mqttc::Subscription{"+/b", 0});
    corpus.push_back(mqttc::encode(subscribe).value());

    mqttc::Unsubscribe unsubscribe;
    unsubscribe.packet_id = 11;
    unsubscribe.filters.push_back("a/#");
    corpus.push_back(mqttc::encode(unsubscribe).value());

    mqttc::ConnAck connack;
    connack.session_present = true;
    connack.return_code = 3;
    corpus.push_back(mqttc::encode(connack).value());

    mqttc::SubAck suback;
    suback.packet_id = 9;
    suback.return_codes = {0x00, 0x01, 0x80};
    corpus.push_back(mqttc::encode(suback).value());

    corpus.push_back(mqttc::encode(mqttc::PingReq{}).value());
    corpus.push_back(mqttc::encode(mqttc::Disconnect{}).value());
    return corpus;
}

// 解出来的报文必须能再编回去，且规范化前后语义一致。
// 注意 decode(data, size) 的契约是 data[0..size) 全部可读：调用方必须先确认
// "声明长度 <= 实际缓冲"，否则那是调用方越界，不是解码器的问题。
//
// 不能要求"再编码的字节与原文逐字节相同"：MQTT 3.1.1 没要求 Remaining Length
// 用最短编码（5.0 才要求），解码器接受 0x80 0x00 表示 0，而编码器永远输出最短形式。
// 这个错不变量是 Linux 上的 libFuzzer 先撞出来的（输入 0xC0 0x80 0x00）。
void check_round_trip(const mqttc::ByteVec& bytes, size_t length) {
    if (length > bytes.size()) return;  // 半包：真实调用方会等更多字节，不该硬解

    auto packet = mqttc::decode(bytes.data(), length);
    if (!packet.ok()) return;

    auto reencoded = std::visit([](const auto& value) { return mqttc::encode(value); },
                                packet.value());
    REQUIRE(reencoded.ok());
    CHECK(reencoded.value().size() <= length);  // 规范化只会变短，不会变长

    // 再解码必须还是同一类报文，且规范化形式幂等
    auto again = mqttc::decode(reencoded.value().data(), reencoded.value().size());
    REQUIRE(again.ok());
    CHECK_EQ(again.value().index(), packet.value().index());

    auto canonical = std::visit([](const auto& value) { return mqttc::encode(value); },
                                again.value());
    REQUIRE(canonical.ok());
    CHECK_EQ(canonical.value(), reencoded.value());
}

} // namespace

TEST(fuzz_lite_decode_survives_random_bytes) {
    Rng rng;
    for (int round = 0; round < 20000; ++round) {
        const size_t size = rng.below(96);
        mqttc::ByteVec data(size);
        for (uint8_t& byte : data) byte = rng.byte();

        auto peeked = mqttc::peek_packet_size(data.data(), data.size());
        if (!peeked.ok()) {
            // peek 只看固定头与长度字段，所以只可能报这三种：字节不够、长度超协议上限、
            // 长度字段本身非法。别的一律算异常。
            CHECK(peeked.error() == mqttc::Error::WouldBlock ||
                  peeked.error() == mqttc::Error::Oversize ||
                  peeked.error() == mqttc::Error::ProtocolError);
            continue;
        }
        // 声明长度必须在协议上限内；能不能解出来是 decode 的事
        CHECK(peeked.value() <= mqttc::kMaxPacketSize);
        check_round_trip(data, peeked.value());
    }
}

TEST(fuzz_lite_decode_survives_mutated_packets) {
    const std::vector<mqttc::ByteVec> corpus = valid_corpus();
    Rng rng{0xDEADBEEF};

    for (int round = 0; round < 20000; ++round) {
        mqttc::ByteVec data = corpus[rng.below(corpus.size())];

        // 随机制造几种脏数据：翻字节、截断、插长长度、改首位
        switch (rng.below(4)) {
        case 0:
            for (int flips = 1 + static_cast<int>(rng.below(4)); flips > 0; --flips) {
                if (!data.empty()) data[rng.below(data.size())] = rng.byte();
            }
            break;
        case 1:
            data.resize(rng.below(data.size() + 1));
            break;
        case 2:
            data.push_back(0xFF);
            data.push_back(0xFF);
            data.push_back(0xFF);
            data.push_back(0xFF);
            data.push_back(0xFF);
            break;
        default:
            if (!data.empty()) data[0] = rng.byte();
            break;
        }

        auto peeked = mqttc::peek_packet_size(data.data(), data.size());
        if (peeked.ok() && peeked.value() <= data.size()) {
            check_round_trip(data, peeked.value());
        }
    }
}

TEST(fuzz_lite_topic_validation_survives_random_strings) {
    Rng rng;
    const std::string alphabet = "ab/#+$ \xC3\xA9\x01";

    for (int round = 0; round < 20000; ++round) {
        std::string text;
        const size_t length = rng.below(12);
        for (size_t i = 0; i < length; ++i) {
            const char c = alphabet[rng.below(alphabet.size())];
            text.push_back(c == '\x01' ? '\0' : c);  // 顺带塞进空字节
        }

        // 校验结果必须稳定：同一个串两次结果一样
        const bool as_name = mqttc::valid_topic_name(text);
        const bool as_filter = mqttc::valid_topic_filter(text);
        CHECK_EQ(mqttc::valid_topic_name(text), as_name);
        CHECK_EQ(mqttc::valid_topic_filter(text), as_filter);

        // 合法的主题名必然也是合法的过滤器（过滤器是主题名的超集）
        if (as_name) CHECK(as_filter);
    }
}

TEST(fuzz_lite_valid_utf8_survives_random_bytes) {
    Rng rng;
    for (int round = 0; round < 20000; ++round) {
        const size_t size = rng.below(16);
        std::string text;
        text.reserve(size);
        for (size_t i = 0; i < size; ++i) text.push_back(static_cast<char>(rng.byte()));

        const bool result = mqttc::wire::valid_utf8(text);
        CHECK_EQ(mqttc::wire::valid_utf8(text), result);  // 稳定
    }
}

TEST(fuzz_lite_rejects_a_declared_length_that_is_never_satisfied) {
    // "声明 200 MB，只给 3 个字节"：不许去分配那 200 MB
    mqttc::ByteVec header{0x30, 0xFF, 0xFF, 0xFF, 0x7F};  // PUBLISH, Remaining Length = 268435455
    auto peeked = mqttc::peek_packet_size(header.data(), header.size());
    REQUIRE(peeked.ok());
    CHECK_EQ(peeked.value(), static_cast<size_t>(268435455) + 5);
    CHECK(peeked.value() > header.size());  // 调用方据此知道"还不够，继续等"
}
