// libFuzzer 目标：UTF-8 校验、主题校验、Remaining Length 的变长整数。
//
// 不变量：
// 1. valid_utf8 不能因为输入里的裸空字节、超长序列、孤立续字节而崩或读越界；
// 2. valid_topic_name ⇒ valid_topic_filter（过滤器是主题名的超集）；
// 3. varint 解出来的值不超过协议上限（4 字节）；重新编码后能解回同一个值，
//    且再编码不长于原文。注意不能要求逐字节相同：3.1.1 没要求最短编码
//    （FF FF 00 与 FF 7F 都表示 16383），编码器统一输出最短形式。
//
// 构建：-DMQTTC_BUILD_FUZZERS=ON（需要 clang 与 libclang-rt 的 fuzzer 运行时）。

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "mqttc/topic.hpp"
#include "mqttc/wire.hpp"

using namespace mqttc;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const std::string_view text(reinterpret_cast<const char*>(data), size);

    const bool as_name = valid_topic_name(text);
    const bool as_filter = valid_topic_filter(text);
    assert((!as_name || as_filter) && "主题名合法但过滤器不合法");

    const bool utf8 = wire::valid_utf8(text);
    (void)utf8;

    // 前 4 个字节当 Remaining Length 编码解一次，再编回去做语义往返
    wire::Reader reader(data, size);
    auto value = reader.varint();
    if (value.ok()) {
        const size_t consumed = static_cast<size_t>(reader.pos() - data);
        ByteVec encoded;
        assert(wire::append_varint(encoded, value.value()) && "协议上限内的值编不回去");
        assert(encoded.size() <= consumed && "varint 再编码比原编码还长");

        wire::Reader again(encoded.data(), encoded.size());
        auto round_tripped = again.varint();
        assert(round_tripped.ok() && round_tripped.value() == value.value() &&
               "varint 编解码不是互逆的");
    }

    return 0;
}
