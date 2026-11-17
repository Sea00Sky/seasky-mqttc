#include "mqttc/packet.hpp"

#include <utility>

#include "mqttc/topic.hpp"

namespace mqttc {
namespace {

constexpr uint8_t kTypeConnect = 1;
constexpr uint8_t kTypeConnAck = 2;
constexpr uint8_t kTypePublish = 3;
constexpr uint8_t kTypePubAck = 4;
constexpr uint8_t kTypeSubscribe = 8;
constexpr uint8_t kTypeSubAck = 9;
constexpr uint8_t kTypeUnsubscribe = 10;
constexpr uint8_t kTypeUnsubAck = 11;
constexpr uint8_t kTypePingReq = 12;
constexpr uint8_t kTypePingResp = 13;
constexpr uint8_t kTypeDisconnect = 14;

constexpr size_t kMaxStringLength = 65535;

// 固定头标志位（除 PUBLISH 外都是常量）
constexpr uint8_t kSubscribeFlags = 0x02;
constexpr uint8_t kUnsubscribeFlags = 0x02;

Result<ByteVec> fail(Error error) { return Result<ByteVec>::failure(error); }
Result<Packet> fail_packet(Error error) { return Result<Packet>::failure(error); }

// 缓冲区已按声明长度取全，此时再缺字节说明报文本身非法，不是等更多数据
Error as_protocol_error(Error error) {
    return error == Error::WouldBlock ? Error::ProtocolError : error;
}

Result<ByteVec> finish(uint8_t first_byte, ByteVec body) {
    if (body.size() > wire::kMaxRemainingLength) return fail(Error::Oversize);

    ByteVec out;
    out.reserve(body.size() + 5);
    out.push_back(first_byte);
    if (!wire::append_varint(out, static_cast<uint32_t>(body.size()))) return fail(Error::Oversize);
    out.insert(out.end(), body.begin(), body.end());
    return Result<ByteVec>::success(std::move(out));
}

// 0x02 表示授予 QoS2：协议合法，但本库不做 QoS2，解到即判协议错误
bool valid_return_code(uint8_t code) { return code == 0x00 || code == 0x01 || code == 0x80; }

// ---------------------------------------------------------------- decode 辅助

Result<Packet> decode_connect(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_connack(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_publish(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_puback(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_subscribe(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_suback(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_unsubscribe(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_unsuback(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_ping_req(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_ping_resp(wire::Reader& reader, uint8_t flags);
Result<Packet> decode_disconnect(wire::Reader& reader, uint8_t flags);

} // namespace

// ---------------------------------------------------------------- CONNECT

Result<ByteVec> encode(const Connect& packet) {
    if (packet.client_id.size() > kMaxStringLength) return fail(Error::Oversize);
    if (packet.client_id.empty() && !packet.clean_session) return fail(Error::ProtocolError);
    if (!wire::valid_utf8(packet.client_id)) return fail(Error::ProtocolError);

    uint8_t flags = packet.clean_session ? 0x02 : 0x00;
    if (packet.will) {
        const Will& will = *packet.will;
        if (will.qos > kMaxSupportedQos) return fail(Error::ProtocolError);
        if (will.topic.size() > kMaxStringLength || will.payload.size() > kMaxStringLength) {
            return fail(Error::Oversize);
        }
        if (!valid_topic_name(will.topic)) return fail(Error::ProtocolError);
        flags |= 0x04;
        if (will.retain) flags |= 0x20;
        flags |= static_cast<uint8_t>(will.qos << 3);
    }
    if (packet.username) {
        if (packet.username->size() > kMaxStringLength) return fail(Error::Oversize);
        if (!wire::valid_utf8(*packet.username)) return fail(Error::ProtocolError);
        flags |= 0x80;
    }
    if (packet.password) {
        if (!packet.username) return fail(Error::ProtocolError);  // 密码位不能单独置位
        if (packet.password->size() > kMaxStringLength) return fail(Error::Oversize);
        if (!wire::valid_utf8(*packet.password)) return fail(Error::ProtocolError);
        flags |= 0x40;
    }

    ByteVec body;
    wire::Writer writer(body);
    writer.string("MQTT");
    writer.u8(4);
    writer.u8(flags);
    writer.u16(packet.keepalive);
    writer.string(packet.client_id);
    if (packet.will) {
        const Will& will = *packet.will;
        writer.string(will.topic);
        writer.u16(static_cast<uint16_t>(will.payload.size()));
        writer.raw(will.payload);
    }
    if (packet.username) writer.string(*packet.username);
    if (packet.password) writer.string(*packet.password);

    return finish(0x10, std::move(body));
}

namespace {

Result<Packet> decode_connect(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00) return fail_packet(Error::ProtocolError);

    auto protocol = reader.string();
    if (!protocol) return fail_packet(as_protocol_error(protocol.error()));
    if (protocol.value() != "MQTT") return fail_packet(Error::ProtocolError);

    auto level = reader.u8();
    if (!level) return fail_packet(as_protocol_error(level.error()));
    if (level.value() != 4) return fail_packet(Error::ProtocolError);

    auto connect_flags = reader.u8();
    if (!connect_flags) return fail_packet(as_protocol_error(connect_flags.error()));
    const uint8_t bits = connect_flags.value();
    if ((bits & 0x01) != 0) return fail_packet(Error::ProtocolError);  // 保留位必须为 0

    const bool has_username = (bits & 0x80) != 0;
    const bool has_password = (bits & 0x40) != 0;
    const bool has_will = (bits & 0x04) != 0;
    const uint8_t will_qos = static_cast<uint8_t>((bits >> 3) & 0x03);
    const bool will_retain = (bits & 0x20) != 0;

    if (has_password && !has_username) return fail_packet(Error::ProtocolError);
    if (!has_will && (will_qos != 0 || will_retain)) return fail_packet(Error::ProtocolError);
    if (will_qos > kMaxSupportedQos) return fail_packet(Error::ProtocolError);

    auto keepalive = reader.u16();
    if (!keepalive) return fail_packet(as_protocol_error(keepalive.error()));

    auto client_id = reader.string();
    if (!client_id) return fail_packet(as_protocol_error(client_id.error()));

    Connect packet;
    packet.client_id.assign(client_id.value());
    packet.clean_session = (bits & 0x02) != 0;
    packet.keepalive = keepalive.value();
    if (packet.client_id.empty() && !packet.clean_session) return fail_packet(Error::ProtocolError);

    if (has_will) {
        Will will;
        auto topic = reader.string();
        if (!topic) return fail_packet(as_protocol_error(topic.error()));
        if (!valid_topic_name(topic.value())) return fail_packet(Error::ProtocolError);
        will.topic.assign(topic.value());

        auto length = reader.u16();
        if (!length) return fail_packet(as_protocol_error(length.error()));
        auto payload = reader.bytes(length.value());
        if (!payload) return fail_packet(as_protocol_error(payload.error()));

        will.payload = payload.take();
        will.qos = will_qos;
        will.retain = will_retain;
        packet.will = std::move(will);
    }
    if (has_username) {
        auto username = reader.string();
        if (!username) return fail_packet(as_protocol_error(username.error()));
        packet.username = std::string(username.value());
    }
    if (has_password) {
        auto password = reader.string();
        if (!password) return fail_packet(as_protocol_error(password.error()));
        packet.password = std::string(password.value());
    }
    if (!reader.empty()) return fail_packet(Error::ProtocolError);

    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- CONNACK

Result<Packet> decode_connack(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00) return fail_packet(Error::ProtocolError);

    auto ack_flags = reader.u8();
    if (!ack_flags) return fail_packet(as_protocol_error(ack_flags.error()));
    if ((ack_flags.value() & 0xFE) != 0) return fail_packet(Error::ProtocolError);

    auto code = reader.u8();
    if (!code) return fail_packet(as_protocol_error(code.error()));
    if (code.value() > 5) return fail_packet(Error::ProtocolError);
    if (!reader.empty()) return fail_packet(Error::ProtocolError);

    ConnAck packet;
    packet.session_present = (ack_flags.value() & 0x01) != 0;
    packet.return_code = code.value();
    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- PUBLISH

Result<Packet> decode_publish(wire::Reader& reader, uint8_t flags) {
    const uint8_t qos = static_cast<uint8_t>((flags >> 1) & 0x03);
    const bool dup = (flags & 0x08) != 0;
    const bool retain = (flags & 0x01) != 0;

    if (qos > kMaxSupportedQos) return fail_packet(Error::ProtocolError);  // 含 QoS 3
    if (qos == 0 && dup) return fail_packet(Error::ProtocolError);

    auto topic = reader.string();
    if (!topic) return fail_packet(as_protocol_error(topic.error()));
    if (!valid_topic_name(topic.value())) return fail_packet(Error::ProtocolError);

    Publish packet;
    packet.topic.assign(topic.value());
    packet.qos = qos;
    packet.dup = dup;
    packet.retain = retain;

    if (qos > 0) {
        auto packet_id = reader.u16();
        if (!packet_id) return fail_packet(as_protocol_error(packet_id.error()));
        if (packet_id.value() == 0) return fail_packet(Error::ProtocolError);
        packet.packet_id = packet_id.value();
    }

    auto payload = reader.bytes(reader.remaining());
    if (!payload) return fail_packet(as_protocol_error(payload.error()));
    packet.payload = payload.take();

    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- PUBACK

Result<Packet> decode_puback(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00) return fail_packet(Error::ProtocolError);

    auto packet_id = reader.u16();
    if (!packet_id) return fail_packet(as_protocol_error(packet_id.error()));
    if (packet_id.value() == 0) return fail_packet(Error::ProtocolError);
    if (!reader.empty()) return fail_packet(Error::ProtocolError);

    PubAck packet;
    packet.packet_id = packet_id.value();
    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- SUBSCRIBE

Result<Packet> decode_subscribe(wire::Reader& reader, uint8_t flags) {
    if (flags != kSubscribeFlags) return fail_packet(Error::ProtocolError);

    auto packet_id = reader.u16();
    if (!packet_id) return fail_packet(as_protocol_error(packet_id.error()));
    if (packet_id.value() == 0) return fail_packet(Error::ProtocolError);

    Subscribe packet;
    packet.packet_id = packet_id.value();

    while (!reader.empty()) {
        auto filter = reader.string();
        if (!filter) return fail_packet(as_protocol_error(filter.error()));
        if (!valid_topic_filter(filter.value())) return fail_packet(Error::ProtocolError);

        auto qos = reader.u8();
        if (!qos) return fail_packet(as_protocol_error(qos.error()));
        if (qos.value() > kMaxSupportedQos) return fail_packet(Error::ProtocolError);

        Subscription entry;
        entry.filter.assign(filter.value());
        entry.qos = qos.value();
        packet.entries.push_back(std::move(entry));
    }
    if (packet.entries.empty()) return fail_packet(Error::ProtocolError);

    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- SUBACK

Result<Packet> decode_suback(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00) return fail_packet(Error::ProtocolError);

    auto packet_id = reader.u16();
    if (!packet_id) return fail_packet(as_protocol_error(packet_id.error()));
    if (packet_id.value() == 0) return fail_packet(Error::ProtocolError);

    SubAck packet;
    packet.packet_id = packet_id.value();

    while (!reader.empty()) {
        auto code = reader.u8();
        if (!code) return fail_packet(as_protocol_error(code.error()));
        if (!valid_return_code(code.value())) return fail_packet(Error::ProtocolError);
        packet.return_codes.push_back(code.value());
    }
    if (packet.return_codes.empty()) return fail_packet(Error::ProtocolError);

    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- UNSUBSCRIBE

Result<Packet> decode_unsubscribe(wire::Reader& reader, uint8_t flags) {
    if (flags != kUnsubscribeFlags) return fail_packet(Error::ProtocolError);

    auto packet_id = reader.u16();
    if (!packet_id) return fail_packet(as_protocol_error(packet_id.error()));
    if (packet_id.value() == 0) return fail_packet(Error::ProtocolError);

    Unsubscribe packet;
    packet.packet_id = packet_id.value();

    while (!reader.empty()) {
        auto filter = reader.string();
        if (!filter) return fail_packet(as_protocol_error(filter.error()));
        if (!valid_topic_filter(filter.value())) return fail_packet(Error::ProtocolError);
        packet.filters.emplace_back(filter.value());
    }
    if (packet.filters.empty()) return fail_packet(Error::ProtocolError);

    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- UNSUBACK

Result<Packet> decode_unsuback(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00) return fail_packet(Error::ProtocolError);

    auto packet_id = reader.u16();
    if (!packet_id) return fail_packet(as_protocol_error(packet_id.error()));
    if (packet_id.value() == 0) return fail_packet(Error::ProtocolError);
    if (!reader.empty()) return fail_packet(Error::ProtocolError);

    UnsubAck packet;
    packet.packet_id = packet_id.value();
    return Result<Packet>::success(Packet{std::move(packet)});
}

// ---------------------------------------------------------------- 无负载报文

Result<Packet> decode_ping_req(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00 || !reader.empty()) return fail_packet(Error::ProtocolError);
    return Result<Packet>::success(Packet{PingReq{}});
}

Result<Packet> decode_ping_resp(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00 || !reader.empty()) return fail_packet(Error::ProtocolError);
    return Result<Packet>::success(Packet{PingResp{}});
}

Result<Packet> decode_disconnect(wire::Reader& reader, uint8_t flags) {
    if (flags != 0x00 || !reader.empty()) return fail_packet(Error::ProtocolError);
    return Result<Packet>::success(Packet{Disconnect{}});
}

} // namespace

// ---------------------------------------------------------------- 其余 encode

Result<ByteVec> encode(const ConnAck& packet) {
    if (packet.return_code > 5) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.u8(packet.session_present ? 0x01 : 0x00);
    writer.u8(packet.return_code);
    return finish(0x20, std::move(body));
}

Result<ByteVec> encode(const Publish& packet) {
    if (packet.topic.size() > kMaxStringLength) return fail(Error::Oversize);
    if (!valid_topic_name(packet.topic)) return fail(Error::ProtocolError);
    if (packet.qos > kMaxSupportedQos) return fail(Error::ProtocolError);
    if (packet.qos == 0 && packet.dup) return fail(Error::ProtocolError);
    if (packet.qos > 0 && packet.packet_id == 0) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.string(packet.topic);
    if (packet.qos > 0) writer.u16(packet.packet_id);
    writer.raw(packet.payload);

    uint8_t first_byte = 0x30;
    if (packet.dup) first_byte |= 0x08;
    first_byte |= static_cast<uint8_t>(packet.qos << 1);
    if (packet.retain) first_byte |= 0x01;
    return finish(first_byte, std::move(body));
}

Result<ByteVec> encode(const PubAck& packet) {
    if (packet.packet_id == 0) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.u16(packet.packet_id);
    return finish(0x40, std::move(body));
}

Result<ByteVec> encode(const Subscribe& packet) {
    if (packet.packet_id == 0) return fail(Error::ProtocolError);
    if (packet.entries.empty()) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.u16(packet.packet_id);
    for (const Subscription& entry : packet.entries) {
        if (entry.filter.size() > kMaxStringLength) return fail(Error::Oversize);
        if (!valid_topic_filter(entry.filter)) return fail(Error::ProtocolError);
        if (entry.qos > kMaxSupportedQos) return fail(Error::ProtocolError);
        writer.string(entry.filter);
        writer.u8(entry.qos);
    }
    return finish(0x82, std::move(body));
}

Result<ByteVec> encode(const SubAck& packet) {
    if (packet.packet_id == 0) return fail(Error::ProtocolError);
    if (packet.return_codes.empty()) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.u16(packet.packet_id);
    for (uint8_t code : packet.return_codes) {
        if (!valid_return_code(code)) return fail(Error::ProtocolError);
        writer.u8(code);
    }
    return finish(0x90, std::move(body));
}

Result<ByteVec> encode(const Unsubscribe& packet) {
    if (packet.packet_id == 0) return fail(Error::ProtocolError);
    if (packet.filters.empty()) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.u16(packet.packet_id);
    for (const std::string& filter : packet.filters) {
        if (filter.size() > kMaxStringLength) return fail(Error::Oversize);
        if (!valid_topic_filter(filter)) return fail(Error::ProtocolError);
        writer.string(filter);
    }
    return finish(0xA2, std::move(body));
}

Result<ByteVec> encode(const UnsubAck& packet) {
    if (packet.packet_id == 0) return fail(Error::ProtocolError);

    ByteVec body;
    wire::Writer writer(body);
    writer.u16(packet.packet_id);
    return finish(0xB0, std::move(body));
}

Result<ByteVec> encode(const PingReq&) { return finish(0xC0, ByteVec{}); }
Result<ByteVec> encode(const PingResp&) { return finish(0xD0, ByteVec{}); }
Result<ByteVec> encode(const Disconnect&) { return finish(0xE0, ByteVec{}); }

// ---------------------------------------------------------------- decode

Result<size_t> peek_packet_size(const uint8_t* data, size_t size) {
    if (size < 2) return Result<size_t>::failure(Error::WouldBlock);

    wire::Reader length_reader(data + 1, size - 1);
    auto length = length_reader.varint();
    if (!length) return Result<size_t>::failure(length.error());

    const size_t header = 1 + static_cast<size_t>(length_reader.pos() - (data + 1));
    return Result<size_t>::success(header + static_cast<size_t>(length.value()));
}

Result<Packet> decode(const uint8_t* data, size_t size) {
    auto total = peek_packet_size(data, size);
    if (!total) return fail_packet(total.error());
    if (size < total.value()) return fail_packet(Error::WouldBlock);
    if (size > total.value()) return fail_packet(Error::ProtocolError);

    const uint8_t first_byte = data[0];
    const uint8_t type = first_byte >> 4;
    const uint8_t flags = first_byte & 0x0F;

    wire::Reader length_reader(data + 1, size - 1);
    auto length = length_reader.varint();
    if (!length) return fail_packet(as_protocol_error(length.error()));
    const size_t header = 1 + static_cast<size_t>(length_reader.pos() - (data + 1));

    wire::Reader reader(data + header, size - header);
    switch (type) {
    case kTypeConnect:
        return decode_connect(reader, flags);
    case kTypeConnAck:
        return decode_connack(reader, flags);
    case kTypePublish:
        return decode_publish(reader, flags);
    case kTypePubAck:
        return decode_puback(reader, flags);
    case kTypeSubscribe:
        return decode_subscribe(reader, flags);
    case kTypeSubAck:
        return decode_suback(reader, flags);
    case kTypeUnsubscribe:
        return decode_unsubscribe(reader, flags);
    case kTypeUnsubAck:
        return decode_unsuback(reader, flags);
    case kTypePingReq:
        return decode_ping_req(reader, flags);
    case kTypePingResp:
        return decode_ping_resp(reader, flags);
    case kTypeDisconnect:
        return decode_disconnect(reader, flags);
    default:
        return fail_packet(Error::ProtocolError);  // 0 与 15 是保留值
    }
}

Result<Packet> decode(const ByteVec& raw) { return decode(raw.data(), raw.size()); }

} // namespace mqttc
