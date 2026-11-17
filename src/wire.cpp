#include "mqttc/wire.hpp"

namespace mqttc {
namespace wire {
namespace {

bool is_continuation(uint8_t byte) { return (byte & 0xC0) == 0x80; }

} // namespace

bool valid_utf8(std::string_view text) {
    const auto* data = reinterpret_cast<const uint8_t*>(text.data());
    const size_t size = text.size();
    size_t i = 0;

    while (i < size) {
        const uint8_t lead = data[i];

        if (lead < 0x80) {
            if (lead == 0x00) return false;  // U+0000 不允许
            ++i;
        } else if ((lead & 0xE0) == 0xC0) {
            if (lead < 0xC2) return false;  // 过长编码
            if (i + 1 >= size || !is_continuation(data[i + 1])) return false;
            i += 2;
        } else if ((lead & 0xF0) == 0xE0) {
            if (i + 2 >= size) return false;
            const uint8_t b1 = data[i + 1];
            if (!is_continuation(b1) || !is_continuation(data[i + 2])) return false;
            if (lead == 0xE0 && b1 < 0xA0) return false;  // 过长编码
            if (lead == 0xED && b1 > 0x9F) return false;  // 代理对
            i += 3;
        } else if ((lead & 0xF8) == 0xF0) {
            if (i + 3 >= size) return false;
            const uint8_t b1 = data[i + 1];
            if (!is_continuation(b1) || !is_continuation(data[i + 2]) || !is_continuation(data[i + 3])) {
                return false;
            }
            if (lead > 0xF4) return false;
            if (lead == 0xF0 && b1 < 0x90) return false;  // 过长编码
            if (lead == 0xF4 && b1 > 0x8F) return false;  // 超过 U+10FFFF
            i += 4;
        } else {
            return false;  // 孤立续字节或非法首字节
        }
    }
    return true;
}

bool append_varint(ByteVec& out, uint32_t value) {
    if (value > kMaxRemainingLength) return false;
    do {
        uint8_t byte = static_cast<uint8_t>(value & 0x7F);
        value >>= 7;
        if (value != 0) byte |= 0x80;
        out.push_back(byte);
    } while (value != 0);
    return true;
}

void Writer::raw(const uint8_t* data, size_t size) {
    if (size == 0) return;
    out_.insert(out_.end(), data, data + size);
}

void Writer::string(std::string_view text) {
    u16(static_cast<uint16_t>(text.size()));
    out_.insert(out_.end(), text.begin(), text.end());
}

Result<uint8_t> Reader::u8() {
    if (pos_ == end_) return Result<uint8_t>::failure(Error::WouldBlock);
    return Result<uint8_t>::success(*pos_++);
}

Result<uint16_t> Reader::u16() {
    auto high = u8();
    if (!high) return Result<uint16_t>::failure(high.error());
    auto low = u8();
    if (!low) return Result<uint16_t>::failure(low.error());
    const uint16_t value =
        static_cast<uint16_t>((static_cast<uint16_t>(high.value()) << 8) | low.value());
    return Result<uint16_t>::success(value);
}

Result<uint32_t> Reader::varint() {
    uint32_t value = 0;
    uint32_t weight = 1;
    for (int i = 0; i < 4; ++i) {
        auto byte = u8();
        if (!byte) return Result<uint32_t>::failure(byte.error());
        value += static_cast<uint32_t>(byte.value() & 0x7F) * weight;
        if ((byte.value() & 0x80) == 0) return Result<uint32_t>::success(value);
        weight <<= 7;
    }
    return Result<uint32_t>::failure(Error::ProtocolError);  // 第五个字节：超长
}

Result<std::string_view> Reader::string() {
    auto length = u16();
    if (!length) return Result<std::string_view>::failure(length.error());
    if (remaining() < length.value()) return Result<std::string_view>::failure(Error::WouldBlock);

    const std::string_view text(reinterpret_cast<const char*>(pos_), length.value());
    if (!valid_utf8(text)) return Result<std::string_view>::failure(Error::ProtocolError);
    pos_ += length.value();
    return Result<std::string_view>::success(text);
}

Result<ByteVec> Reader::bytes(size_t count) {
    if (remaining() < count) return Result<ByteVec>::failure(Error::WouldBlock);
    ByteVec out(pos_, pos_ + count);
    pos_ += count;
    return Result<ByteVec>::success(std::move(out));
}

} // namespace wire
} // namespace mqttc
