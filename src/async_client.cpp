// AsyncClient：单线程 Client 之上的所有权外壳。
//
// 加锁规则（避免死锁的唯一约定）：
// - mutex_ 管命令队列、停止位、快照、rejected_commands_；
// - inbox_mutex_ 管入站队列与它的两个计数；
// - handler_mutex_ 只用于取/换 handler；
// - 三者互不嵌套，除了"投递线程持 inbox_mutex_ 时取 handler_mutex_"这一处单向顺序；
// - 用户回调在**不持任何锁**的状态下执行。

#include "mqttc/async_client.hpp"

#include <cassert>
#include <chrono>
#include <utility>

#include "mqttc/log.hpp"
#include "mqttc/topic.hpp"

namespace mqttc {

AsyncClient::AsyncClient(std::unique_ptr<Transport> transport, ClientOptions options,
                         MessageHandler handler)
    : transport_(std::move(transport)),
      options_(std::move(options)),
      initial_handler_(std::move(handler)) {
    // transport_ 先构造好，Client 拿到的引用才有效；用户回调换成本类的转发器，
    // 真正的用户回调存在 handler_ 里，由投递线程调用
    client_ = std::make_unique<Client>(
        *transport_, options_,
        [this](const Message& message) { return enqueue_inbound(message); });
}

AsyncClient::~AsyncClient() { stop(); }

void AsyncClient::start() {
    if (started_) return;  // 一个对象只跑一轮：重连交给核心的自动重连
    started_ = true;
    running_ = true;

    {
        std::lock_guard<std::mutex> lock(handler_mutex_);
        handler_ = std::make_shared<const MessageHandler>(initial_handler_);
    }

    io_thread_ = std::thread([this] { io_loop(); });
    delivery_thread_ = std::thread([this] { delivery_loop(); });
}

void AsyncClient::stop() {
    assert((!delivery_thread_.joinable() ||
            std::this_thread::get_id() != delivery_thread_.get_id()) &&
           "不要在消息回调里调 stop()：那等于让投递线程 join 自己");

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) return;  // 没跑过，没有线程要 join
        if (stop_) return;      // 幂等；同时只有一个线程能走到 join
        stop_ = true;
    }
    cv_.notify_all();
    if (io_thread_.joinable()) io_thread_.join();

    // IO 线程已经不在写了，这时才能宣布"队列不会再增长"，然后让投递线程排空退出
    {
        std::lock_guard<std::mutex> lock(inbox_mutex_);
        inbox_closed_ = true;
    }
    inbox_cv_.notify_all();
    if (delivery_thread_.joinable()) delivery_thread_.join();

    running_ = false;
    update_snapshot(true);
}

bool AsyncClient::running() const { return running_; }

void AsyncClient::set_handler(MessageHandler handler) {
    std::lock_guard<std::mutex> lock(handler_mutex_);
    handler_ = std::make_shared<const MessageHandler>(std::move(handler));
}

// ------------------------------------------------------------------ 应用线程侧

Error AsyncClient::publish(const std::string& topic, ByteVec payload, QoS qos, bool retain) {
    if (!valid_topic_name(topic)) return Error::ProtocolError;  // 纯函数，调用线程就能判

    Command command;
    command.kind = CommandKind::Publish;
    command.topic = topic;
    command.payload = std::move(payload);
    command.qos = qos;
    command.retain = retain;
    return enqueue_command(std::move(command));
}

Error AsyncClient::subscribe(std::string_view filter, QoS qos) {
    if (!valid_topic_filter(filter)) return Error::ProtocolError;

    Command command;
    command.kind = CommandKind::Subscribe;
    command.topic = std::string(filter);
    command.qos = qos;
    return enqueue_command(std::move(command));
}

Error AsyncClient::unsubscribe(std::string_view filter) {
    if (!valid_topic_filter(filter)) return Error::ProtocolError;

    Command command;
    command.kind = CommandKind::Unsubscribe;
    command.topic = std::string(filter);
    return enqueue_command(std::move(command));
}

Error AsyncClient::enqueue_command(Command command) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ || stop_) return Error::Closed;
    if (snapshot_.state != State::Connected) return Error::Closed;
    if (commands_.size() >= options_.command_queue_capacity) return Error::Busy;

    commands_.push_back(std::move(command));
    command_pending_ = true;
    cv_.notify_all();
    return Error::None;
}

AsyncSnapshot AsyncClient::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_;
}

size_t AsyncClient::queued_inbound() const {
    std::lock_guard<std::mutex> lock(inbox_mutex_);
    return inbox_.size();
}

bool AsyncClient::wait_for_state(State desired, uint32_t timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    return snapshot_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                [&] { return snapshot_.state == desired; });
}

// ------------------------------------------------------------------ IO 线程

void AsyncClient::io_loop() {
    client_->start(monotonic_ms());
    update_snapshot(true);

    for (;;) {
        run_commands();
        client_->pump(monotonic_ms());
        update_snapshot(true);

        std::unique_lock<std::mutex> lock(mutex_);
        if (stop_) break;
        cv_.wait_for(lock, std::chrono::milliseconds(options_.tick_ms),
                     [this] { return stop_ || command_pending_; });
        if (stop_) break;
    }

    run_commands();    // 停机前把已经答应过的命令处理掉（没连上就按 Closed 计）
    client_->stop();   // 在本线程发 DISCONNECT 并关传输层
    update_snapshot(true);
}

void AsyncClient::run_commands() {
    std::deque<Command> batch;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        batch.swap(commands_);
        command_pending_ = false;
    }

    for (Command& command : batch) {
        Error error = Error::None;
        switch (command.kind) {
        case CommandKind::Publish:
            error = client_->publish(command.topic, std::move(command.payload), command.qos,
                                     command.retain);
            break;
        case CommandKind::Subscribe:
            error = client_->subscribe(command.topic, command.qos);
            break;
        case CommandKind::Unsubscribe:
            error = client_->unsubscribe(command.topic);
            break;
        }

        if (error == Error::None) continue;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++rejected_commands_;
        }
        options_.logger.log(LogLevel::Warn,
                            LogLine("command_rejected")
                                .field("topic", command.topic)
                                .field("error", error_name(error))
                                .str());
    }
}

void AsyncClient::update_snapshot(bool notify) {
    size_t queued = 0;
    uint64_t dropped = 0;
    uint64_t delivered = 0;
    {
        std::lock_guard<std::mutex> lock(inbox_mutex_);
        queued = inbox_.size();
        dropped = dropped_inbound_;
        delivered = delivered_;
    }

    AsyncSnapshot fresh;
    fresh.state = client_->state();
    fresh.stats = client_->stats();
    fresh.inflight = client_->inflight_count();
    fresh.subscriptions = client_->subscription_count();
    fresh.queued_inbound = queued;
    fresh.dropped_inbound = dropped;
    fresh.delivered = delivered;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        fresh.rejected_commands = rejected_commands_;
        snapshot_ = fresh;
    }
    if (notify) snapshot_cv_.notify_all();
}

// ------------------------------------------------------------------ 投递线程

void AsyncClient::delivery_loop() {
    for (;;) {
        Message message;
        std::shared_ptr<const MessageHandler> handler;

        {
            std::unique_lock<std::mutex> lock(inbox_mutex_);
            inbox_cv_.wait(lock, [this] { return !inbox_.empty() || inbox_closed_; });
            if (inbox_.empty()) return;  // 已关闭且排空

            message = std::move(inbox_.front());
            inbox_.pop_front();
            inbox_bytes_ -= message.topic.size() + message.payload.size();

            std::lock_guard<std::mutex> handler_lock(handler_mutex_);
            handler = handler_;
        }

        if (!handler || !*handler) continue;
        if (!(*handler)(message)) continue;  // 回调说处理不过来：核心已经计进 dropped

        std::lock_guard<std::mutex> lock(inbox_mutex_);
        ++delivered_;
    }
}

bool AsyncClient::enqueue_inbound(const Message& message) {
    const size_t bytes = message.topic.size() + message.payload.size();
    bool overflowed = false;
    {
        std::lock_guard<std::mutex> lock(inbox_mutex_);
        if (inbox_.size() >= options_.inbound_queue_capacity ||
            inbox_bytes_ + bytes > options_.inbound_max_bytes) {
            ++dropped_inbound_;
            overflowed = true;
        } else {
            inbox_.push_back(message);
            inbox_bytes_ += bytes;
            inbox_cv_.notify_one();
        }
    }

    if (overflowed) {
        // 实时性优先：宁可丢也不无限堆积。核心那边也会因为回调返回 false 记一次 dropped
        options_.logger.log(LogLevel::Warn, LogLine("inbound_overflow")
                                                .field("queued", static_cast<uint64_t>(queued_inbound()))
                                                .field("capacity", static_cast<uint64_t>(options_.inbound_queue_capacity))
                                                .str());
        return false;
    }
    return true;
}

} // namespace mqttc
