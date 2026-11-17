// libFuzzer 目标：任意字节 → 分帧 → 解码 → 重新编码 → 再解码。
//
// 不变量（任何一条不成立就 abort，libFuzzer 会把它当成 crash 报出来）：
// 1. 只要 peek 说"这一包有 N 字节"，decode 吃正好 N 字节就不能越界；
// 2. decode 成功 ⇒ encode 必须成功，且再编码不长于吃进去的那一段
//    （3.1.1 没有要求 Remaining Length 用最短编码——那是 5.0 才加的——
//    所以解码器接受 0x80 0x00 这类写法，编码器则统一输出最短形式；
//    "再编码字节数必须与原文相等"是错的不变量，首轮 fuzz 就把它撞出来了）；
// 3. 再解码必须还是同一类报文，且再编码是幂等的（规范化后收敛到同一种形式）。
//
// 构建：-DMQTTC_BUILD_FUZZERS=ON（需要 clang 与 libclang-rt 的 fuzzer 运行时）。

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <variant>

#include "mqttc/packet.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    auto peeked = mqttc::peek_packet_size(data, size);
    if (!peeked.ok()) return 0;          // 长度字段不完整/非法：正常返回
    if (peeked.value() > size) return 0; // 只给了半包：等更多数据

    auto packet = mqttc::decode(data, peeked.value());
    if (!packet.ok()) return 0;

    auto reencoded = std::visit([](const auto& value) { return mqttc::encode(value); },
                               packet.value());
    assert(reencoded.ok() && "decode 成功但 encode 失败");
    assert(reencoded.value().size() <= peeked.value() && "再编码比原来的字节还长");

    // 再解一次必须还是同一类报文
    auto again = mqttc::decode(reencoded.value().data(), reencoded.value().size());
    assert(again.ok() && "自己编出来的字节自己解不开");
    assert(again.value().index() == packet.value().index() && "再解码得到了另一种报文");

    // 规范化必须收敛：把再解码的结果再编码，应该与上一次编码逐字节相同
    auto canonical = std::visit([](const auto& value) { return mqttc::encode(value); },
                                again.value());
    assert(canonical.ok() && canonical.value() == reencoded.value() &&
           "再编码不是幂等的（没有收敛到同一种形式）");

    return 0;
}