#pragma once

// 报文字节层：大端读写与 Remaining Length 变长整数。
// 不碰 socket、不碰时间、不碰全局状态。

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "mqttc/result.hpp"

namespace mqttc {
namespace wire {

using ByteVec = std::vector<uint8_t>;

// 变长整数最多 4 字节，上限 2^28-1
constexpr uint32_t kMaxRemainingLength = 268435455;

// 严格 UTF-8：拒绝 U+0000、过长编码、代理对、越界码点、截断序列
bool valid_utf8(std::string_view text);

// 追加变长整数；超过上限返回 false
bool append_varint(ByteVec& out, uint32_t value);

class Writer {
public:
    explicit Writer(ByteVec& out) : out_(out) {}

    void u8(uint8_t value) { out_.push_back(value); }

    void u16(uint16_t value) {
        u8(static_cast<uint8_t>(value >> 8));
        u8(static_cast<uint8_t>(value & 0xFF));
    }

    void raw(const uint8_t* data, size_t size);
    void raw(const ByteVec& bytes) { raw(bytes.data(), bytes.size()); }

    // 长度前缀由本函数写入；长度上限与 UTF-8 合法性由调用方先校验
    void string(std::string_view text);

    bool varint(uint32_t value) { return append_varint(out_, value); }

private:
    ByteVec& out_;
};

class Reader {
public:
    Reader(const uint8_t* data, size_t size)
        : pos_(data), end_(size == 0 ? data : data + size) {}

    bool empty() const { return pos_ == end_; }
    size_t remaining() const { return static_cast<size_t>(end_ - pos_); }
    const uint8_t* pos() const { return pos_; }

    Result<uint8_t> u8();
    Result<uint16_t> u16();
    Result<uint32_t> varint();
    Result<std::string_view> string();  // 读出并校验 UTF-8
    Result<ByteVec> bytes(size_t count);

private:
    const uint8_t* pos_;
    const uint8_t* end_;
};

} // namespace wire

// 字节缓冲的类型别名，全库统一用这一个
using ByteVec = wire::ByteVec;

} // namespace mqttc
