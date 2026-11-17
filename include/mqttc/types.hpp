#pragma once

// 客户端层的公共类型：QoS、消息、重连与重发参数。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "mqttc/log.hpp"
#include "mqttc/packet.hpp"
#include "mqttc/result.hpp"

namespace mqttc {

enum class QoS : uint8_t {
    AtMostOnce = 0,
    AtLeastOnce = 1,
};

struct Message {
    std::string topic;
    ByteVec payload;
    QoS qos = QoS::AtMostOnce;
    bool retain = false;
};

// 回调返回 false 表示处理不过来：该条丢弃并计数（实时性优先于完整性）
using MessageHandler = std::function<bool(const Message&)>;

struct ClientOptions {
    std::string client_id;
    bool clean_session = true;
    uint16_t keepalive = 60;  // 秒；0 = 不发心跳
    std::optional<Will> will;
    std::optional<std::string> username;
    std::optional<std::string> password;

    uint32_t publish_retry_ms = 5000;   // QoS1 未收到 PUBACK 的重发间隔
    uint32_t reconnect_min_ms = 1000;   // 退避下限
    uint32_t reconnect_max_ms = 30000;  // 退避上限

    // 退避抖动比例：实际等待在 [base × (1-r), base × (1+r)] 之间随机。
    // 没有抖动的话，一批同时掉线的设备会在同一毫秒一起回来打雷。
    double backoff_jitter_ratio = 0.2;
    // 随机源种子。0 = 用单调时钟自动播种（每次运行不同）；测试里显式给一个固定值，
    // 抖动就完全可复现。
    uint64_t rng_seed = 0;

    // 连接尝试次数上限，0 = 一直试。到顶判永久失败（State::Failed）并停止。
    // "尝试"含连不上 socket 的那些（黑洞地址不能让它无限重试）。
    uint32_t max_connect_attempts = 0;
    // 凭据错、标识符被拒这类"再试也没用"的失败要不要照样重试（默认不要）
    bool retry_on_permanent = false;

    // 未确认 QoS1 消息的上限。到顶后 publish() 返回 Busy，由调用方决定阻塞、丢弃还是
    // 自己排队——库不在内部无限堆积。
    uint32_t max_inflight = 1024;

    // 单个报文的上限（字节）。协议允许到 256 MiB，但那是"对端说了算"的数字：
    // 不设这个上限，一个乱报长度的对端就能让本端缓冲 256 MiB。设备侧默认 16 MiB。
    uint32_t max_packet_bytes = 16u * 1024u * 1024u;

    // 日志：默认没装 sink，库里不会打印任何东西（不碰 stdout/stderr）
    Logger logger;

    // ---- 下面几个只被 AsyncClient（异步外壳）用，同步 Client 不看 ----

    // IO 线程两次 pump 之间的间隔。不变量：tick_ms <= min(keepalive_ms/4,
    // publish_retry_ms/4)，否则定时器精度会盖过 keepalive 的裕量。
    uint32_t tick_ms = 10;
    // 入站队列上限：同时限条数与字节数（一条超大消息也不能把内存顶穿）
    size_t inbound_queue_capacity = 256;
    size_t inbound_max_bytes = 4u * 1024u * 1024u;
    // 命令队列上限：满了 publish() 立刻返回 Busy，让调用方自己决定怎么退让
    size_t command_queue_capacity = 1024;
};

enum class State {
    Idle,           // 尚未发起连接
    WaitingConnAck, // CONNECT 已发，等 CONNACK
    Connected,      // 正常收发
    Backoff,        // 掉线后等下一次重连
    Failed,         // 永久失败（凭据错、标识符被拒、试满次数…）：不再自动重试，等调用方处理
};

struct Stats {
    uint64_t published = 0;
    uint64_t received = 0;
    uint64_t dropped = 0;              // 回调返回 false 的次数
    uint64_t resends = 0;              // QoS1 重发次数（含重连后重发）
    uint64_t reconnects = 0;           // 重连成功次数（首次连接不算）
    uint64_t protocol_errors = 0;      // 收到畸形数据的次数
    uint64_t keepalive_timeouts = 0;   // 等不到 PINGRESP 的次数
    uint64_t subscribe_failures = 0;   // SUBACK 返回 0x80 的次数
    uint64_t unexpected_packets = 0;   // 服务端发来不该发的报文
    uint64_t bytes_in = 0;             // 从传输层读到的字节数
    uint64_t bytes_out = 0;            // 写进传输层的字节数
    uint64_t ticks = 0;                // pump() 被调用的次数
    uint64_t oversize_packets = 0;     // 超过 max_packet_bytes 被拒的报文数
    uint64_t permanent_failures = 0;   // 判为永久失败、放弃重试的次数
};

// 稳定的文本快照：一行一个 key=value（实现在 src/log.cpp）
std::string format_stats(const Stats& stats);

} // namespace mqttc
