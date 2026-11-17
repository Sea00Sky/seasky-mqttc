#pragma once

// 异步外壳：把单线程的 Client 包成"后台 IO 线程 + 投递线程 + 两个有界队列"。
//
// 线程规则（TSan 能过的前提，也是每个成员各自的作用域）：
// - 只有 IO 线程碰 Client 与 Transport，别处一律不碰；
// - 应用线程只往命令队列里放东西，连通性/在途上限的最终判断都在 IO 线程做；
// - 用户回调只跑在投递线程上，且调用时不持任何库内锁；
// - state()/stats() 返回**快照**，由 IO 线程每 tick 在锁下更新，最多滞后一个 tick。
//
// 传输层的前提：read() 必须有界（本项目自带的实现都基于 poll/select 超时返回
// WouldBlock）。若某个实现会无限阻塞在 read()，stop() 就要一直等它——stop()
// 不会从别的线程去 close 传输层，因为那会破坏"只有 IO 线程碰 transport"。
//
// 禁止在回调里调 stop()：那是投递线程 join 自己，会死锁（Debug 下会断言）。

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "mqttc/client.hpp"
#include "mqttc/result.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

namespace mqttc {

// 一份一致的读数：分开取 state()/stats() 可能拿到不同 tick 的组合
struct AsyncSnapshot {
    State state = State::Idle;
    Stats stats;
    size_t inflight = 0;
    size_t subscriptions = 0;
    size_t queued_inbound = 0;       // 还在入站队列里没交给回调的消息条数
    uint64_t dropped_inbound = 0;    // 队列满被丢掉的条数（同时计进 stats.dropped）
    uint64_t delivered = 0;          // 回调返回 true 的次数
    uint64_t rejected_commands = 0;  // 排到 IO 线程执行时被拒的命令数（没连上/在途满）
};

class AsyncClient {
public:
    // 自己拥有 transport 与 Client（不持外部引用），析构时先 join 两个线程
    AsyncClient(std::unique_ptr<Transport> transport, ClientOptions options,
                MessageHandler handler = MessageHandler());
    ~AsyncClient();

    AsyncClient(const AsyncClient&) = delete;
    AsyncClient& operator=(const AsyncClient&) = delete;

    // 起线程（幂等）。首次连接由 IO 线程发起；用 wait_for_state(Connected) 等结果
    void start();
    // 停机：置停止位 → 唤醒 → join IO → 通知投递线程把队列排空 → join 投递（幂等）
    void stop();
    bool running() const;

    // 入队的语义（不阻塞）：
    //   None          已入队，不代表已经发出去了
    //   ProtocolError 主题/过滤器非法（在调用线程就判掉，不占队列）
    //   Closed        尚未 start()，或当前快照不是 Connected
    //   Busy          命令队列满，调用方自己决定等、丢还是重试
    Error publish(const std::string& topic, ByteVec payload, QoS qos = QoS::AtMostOnce,
                  bool retain = false);
    Error subscribe(std::string_view filter, QoS qos = QoS::AtMostOnce);
    Error unsubscribe(std::string_view filter);

    // 运行中也能换（投递线程把 handler 拷出来再解锁调用，不会持有旧的回调）
    void set_handler(MessageHandler handler);

    AsyncSnapshot snapshot() const;
    State state() const { return snapshot().state; }
    Stats stats() const { return snapshot().stats; }
    size_t queued_inbound() const;

    // 等到快照里出现某个状态；超时返回 false。测试和 CLI 都用它，不靠 sleep 猜
    bool wait_for_state(State desired, uint32_t timeout_ms);

private:
    enum class CommandKind : uint8_t { Publish, Subscribe, Unsubscribe };

    struct Command {
        CommandKind kind = CommandKind::Publish;
        std::string topic;
        ByteVec payload;
        QoS qos = QoS::AtMostOnce;
        bool retain = false;
    };

    void io_loop();
    void delivery_loop();
    void run_commands();
    void update_snapshot(bool notify);
    Error enqueue_command(Command command);
    // 装到 Client 上的转发器：只入队，队列满返回 false（复用核心既有的 dropped 计数）
    bool enqueue_inbound(const Message& message);

    std::unique_ptr<Transport> transport_;
    ClientOptions options_;  // 外壳自己要用的那几项（tick、队列上限、日志）
    MessageHandler initial_handler_;
    std::unique_ptr<Client> client_;

    std::thread io_thread_;
    std::thread delivery_thread_;

    mutable std::mutex mutex_;  // 命令队列 + 快照 + 停止位
    std::condition_variable cv_;
    std::condition_variable snapshot_cv_;
    std::deque<Command> commands_;
    bool command_pending_ = false;
    bool stop_ = false;
    bool started_ = false;  // 一个对象只跑一轮：stop() 之后不再 start()
    std::atomic<bool> running_{false};
    uint64_t rejected_commands_ = 0;
    AsyncSnapshot snapshot_;

    mutable std::mutex inbox_mutex_;
    std::condition_variable inbox_cv_;
    std::deque<Message> inbox_;
    size_t inbox_bytes_ = 0;
    bool inbox_closed_ = false;
    uint64_t dropped_inbound_ = 0;
    uint64_t delivered_ = 0;

    std::mutex handler_mutex_;
    std::shared_ptr<const MessageHandler> handler_;
};

} // namespace mqttc
