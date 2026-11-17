#pragma once

// 各控制报文的编解码，纯函数：输入结构体或字节，不碰 socket、不碰时间。

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "mqttc/result.hpp"
#include "mqttc/wire.hpp"

namespace mqttc {

// 本库只做 QoS 0 与 1；QoS 2 一律拒绝
constexpr uint8_t kMaxSupportedQos = 1;

// 单个报文的长度上限：固定头 1 字节 + 变长长度 4 字节 + 报文体
constexpr size_t kMaxPacketSize = 1 + 4 + wire::kMaxRemainingLength;

struct Will {
    std::string topic;
    ByteVec payload;
    uint8_t qos = 0;
    bool retain = false;
};

struct Connect {
    std::string client_id;  // clean_session 为 false 时必须非空
    bool clean_session = true;
    uint16_t keepalive = 60;
    std::optional<Will> will;
    std::optional<std::string> username;
    std::optional<std::string> password;  // 只有 username 存在时才合法
};

struct ConnAck {
    bool session_present = false;
    uint8_t return_code = 0;  // 0..5
};

struct Publish {
    std::string topic;
    ByteVec payload;
    uint8_t qos = 0;
    bool dup = false;        // QoS0 时必须为 false
    bool retain = false;
    uint16_t packet_id = 0;  // QoS>0 时必须非零
};

struct PubAck {
    uint16_t packet_id = 0;
};

struct Subscription {
    std::string filter;
    uint8_t qos = 0;
};

struct Subscribe {
    uint16_t packet_id = 0;
    std::vector<Subscription> entries;  // 至少一条
};

struct SubAck {
    uint16_t packet_id = 0;
    std::vector<uint8_t> return_codes;  // 0x00 / 0x01 / 0x80，至少一条（0x02 = 授予 QoS2，不支持）
};

struct Unsubscribe {
    uint16_t packet_id = 0;
    std::vector<std::string> filters;  // 至少一条
};

struct UnsubAck {
    uint16_t packet_id = 0;
};

struct PingReq {};
struct PingResp {};
struct Disconnect {};

using Packet = std::variant<Connect, ConnAck, Publish, PubAck, Subscribe, SubAck, Unsubscribe,
                            UnsubAck, PingReq, PingResp, Disconnect>;

Result<ByteVec> encode(const Connect& packet);
Result<ByteVec> encode(const ConnAck& packet);
Result<ByteVec> encode(const Publish& packet);
Result<ByteVec> encode(const PubAck& packet);
Result<ByteVec> encode(const Subscribe& packet);
Result<ByteVec> encode(const SubAck& packet);
Result<ByteVec> encode(const Unsubscribe& packet);
Result<ByteVec> encode(const UnsubAck& packet);
Result<ByteVec> encode(const PingReq& packet);
Result<ByteVec> encode(const PingResp& packet);
Result<ByteVec> encode(const Disconnect& packet);

// decode 只接受一个完整报文；缓冲区多出字节视为协议错误
Result<Packet> decode(const uint8_t* data, size_t size);
Result<Packet> decode(const ByteVec& raw);

// 只读固定头与变长长度，不碰报文内容；数据不足返回 WouldBlock
Result<size_t> peek_packet_size(const uint8_t* data, size_t size);

} // namespace mqttc
