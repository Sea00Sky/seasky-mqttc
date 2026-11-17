// 异步外壳：后台 IO 线程 + 投递线程 + 两个有界队列。
// 这些用例会用真实线程与真实时间（tick 是毫秒级），断言都留了足够裕量，
// 只锁"行为与不变量"，不锁精确耗时。

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "mqttc/async_client.hpp"
#include "mqttc/client.hpp"
#include "mqttc/packet.hpp"
#include "mqttc/types.hpp"

#include "support.hpp"
#include "test.hpp"

namespace {

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

template <typename Predicate>
bool wait_until(Predicate predicate, uint32_t timeout_ms = 4000) {
    const uint64_t deadline = mqttc::monotonic_ms() + timeout_ms;
    while (mqttc::monotonic_ms() < deadline) {
        if (predicate()) return true;
        sleep_ms(2);
    }
    return predicate();
}

mqttc::ClientOptions async_options() {
    mqttc::ClientOptions options;
    options.client_id = "async-tester";
    options.keepalive = 0;  // 大部分用例不需要心跳
    options.tick_ms = 2;
    options.backoff_jitter_ratio = 0.0;
    return options;
}

mqttc::ByteVec publish_bytes(const std::string& topic) {
    mqttc::Publish packet;
    packet.topic = topic;
    packet.payload = {'x'};
    return support::bytes_of(packet);
}

// 起一个已经连上的 AsyncClient：start() → 等 CONNECT 写出来 → 喂 CONNACK → 等 Connected。
// CONNACK 一定要在 connect() 之后喂：假传输的 connect() 会清空收缓冲（新连接从空缓冲开始）。
struct Connected {
    support::MutexInMemoryTransport* transport = nullptr;  // 归 client 所有，测试只借用
    std::unique_ptr<mqttc::AsyncClient> client;
};

Connected connect_async(mqttc::ClientOptions options, mqttc::MessageHandler handler,
                        uint32_t read_block_ms = 5) {
    Connected out;
    auto transport = std::make_unique<support::MutexInMemoryTransport>(read_block_ms);
    out.transport = transport.get();

    out.client = std::make_unique<mqttc::AsyncClient>(std::move(transport), std::move(options),
                                                      std::move(handler));
    out.client->start();
    if (!wait_until([&] { return out.transport->written().size() > 0; }, 3000)) {
        ::t::report(__FILE__, __LINE__, "等待 CONNECT 写出超时");
    }

    out.transport->feed(support::connack(0x00));
    if (!out.client->wait_for_state(mqttc::State::Connected, 3000)) {
        ::t::report(__FILE__, __LINE__, "等待 Connected 超时");
    }
    return out;
}

} // namespace

TEST(async_connects_and_delivers_messages_on_its_own_thread) {
    const std::thread::id test_thread = std::this_thread::get_id();
    std::mutex received_mutex;
    std::vector<std::string> received;
    std::vector<std::thread::id> callback_threads;

    Connected app = connect_async(async_options(), [&](const mqttc::Message& message) {
        std::lock_guard<std::mutex> lock(received_mutex);
        received.push_back(message.topic);
        callback_threads.push_back(std::this_thread::get_id());
        return true;
    });

    CHECK_EQ(app.client->state(), mqttc::State::Connected);
    CHECK(app.client->running());

    CHECK(app.client->subscribe("demo/#") == mqttc::Error::None);
    CHECK(app.client->publish("demo/a", {'h', 'i'}) == mqttc::Error::None);
    CHECK(wait_until([&] { return app.client->stats().published == 1; }));

    app.transport->feed(publish_bytes("demo/a"));
    CHECK(wait_until([&] { return app.client->snapshot().delivered == 1; }));

    {
        std::lock_guard<std::mutex> lock(received_mutex);
        CHECK_EQ(received.size(), static_cast<size_t>(1));
        CHECK_EQ(received[0], std::string("demo/a"));
    }
    // 回调跑在投递线程上，不是调用线程，也不是 IO 线程
    REQUIRE(callback_threads.size() == 1);
    CHECK(callback_threads[0] != test_thread);
    CHECK_EQ(app.client->stats().received, static_cast<uint64_t>(1));

    app.client->stop();
    CHECK(!app.client->running());
}

TEST(async_publish_and_subscribe_reach_the_wire) {
    Connected app = connect_async(async_options(), mqttc::MessageHandler());

    CHECK(app.client->subscribe("demo/#", mqttc::QoS::AtLeastOnce) == mqttc::Error::None);
    CHECK(app.client->publish("demo/a", {'h', 'i'}, mqttc::QoS::AtLeastOnce) == mqttc::Error::None);

    // 命令真的在 IO 线程上执行了：订阅登记进快照，QoS1 消息挂在途等 PUBACK
    CHECK(wait_until([&] { return app.client->snapshot().subscriptions == 1; }));
    CHECK(wait_until([&] { return app.client->stats().published == 1; }));
    CHECK(wait_until([&] { return app.client->snapshot().inflight == 1; }));

    app.client->stop();
}

TEST(async_rejects_bad_arguments_before_queueing) {
    Connected app = connect_async(async_options(), mqttc::MessageHandler());

    CHECK(app.client->publish("", {'x'}) == mqttc::Error::ProtocolError);
    CHECK(app.client->publish("a/+/b", {'x'}) == mqttc::Error::ProtocolError);  // 发布不能带通配符
    CHECK(app.client->subscribe("a/#/b") == mqttc::Error::ProtocolError);
    CHECK(app.client->unsubscribe("a/#/b") == mqttc::Error::ProtocolError);

    sleep_ms(50);  // 给 IO 线程机会执行（不该有任何命令被排进去）
    CHECK_EQ(app.client->stats().published, static_cast<uint64_t>(0));
    CHECK_EQ(app.client->snapshot().rejected_commands, static_cast<uint64_t>(0));

    app.client->stop();
}

TEST(async_returns_closed_before_start_and_after_stop) {
    mqttc::ClientOptions options = async_options();
    auto transport = std::make_unique<support::MutexInMemoryTransport>(5);
    support::MutexInMemoryTransport* raw = transport.get();
    mqttc::AsyncClient client(std::move(transport), options);

    CHECK(client.publish("a/b", {'x'}) == mqttc::Error::Closed);  // 还没 start()
    CHECK(!client.running());

    client.start();
    CHECK(wait_until([&] { return raw->written().size() > 0; }, 3000));  // CONNECT 已写出
    raw->feed(support::connack(0x00));
    CHECK(client.wait_for_state(mqttc::State::Connected, 3000));

    client.stop();
    CHECK(!client.running());
    CHECK(client.publish("a/b", {'x'}) == mqttc::Error::Closed);

    client.stop();  // 幂等：再调一次不该卡住
}

TEST(async_can_publish_from_many_threads) {
    Connected app = connect_async(async_options(), mqttc::MessageHandler());

    constexpr int kThreads = 4;
    constexpr int kPerThread = 200;
    std::atomic<int> accepted{0};

    std::vector<std::thread> publishers;
    publishers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        publishers.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                mqttc::ByteVec payload{static_cast<uint8_t>(t), static_cast<uint8_t>(i)};
                if (app.client->publish("load/" + std::to_string(t), std::move(payload)) ==
                    mqttc::Error::None) {
                    ++accepted;
                }
            }
        });
    }
    for (std::thread& thread : publishers) thread.join();

    CHECK_EQ(accepted.load(), kThreads * kPerThread);
    CHECK(wait_until([&] { return app.client->stats().published == kThreads * kPerThread; }, 8000));

    app.client->stop();
}

TEST(async_drops_inbound_when_the_inbox_is_full) {
    mqttc::ClientOptions options = async_options();
    options.inbound_queue_capacity = 4;

    constexpr int kMessages = 40;
    std::atomic<int> delivered{0};
    Connected app = connect_async(options, [&](const mqttc::Message&) {
        sleep_ms(20);  // 故意慢：让 IO 线程把队列灌满
        ++delivered;
        return true;
    });

    for (int i = 0; i < kMessages; ++i) {
        app.transport->feed(publish_bytes("burst/" + std::to_string(i)));
    }

    CHECK(wait_until([&] { return app.client->snapshot().dropped_inbound > 0; }, 3000));
    // 一条都不许凭空消失：交给回调的 + 丢掉的 = 喂进去的
    CHECK(wait_until(
        [&] {
            const mqttc::AsyncSnapshot snapshot = app.client->snapshot();
            return snapshot.queued_inbound == 0 && snapshot.delivered + snapshot.dropped_inbound ==
                                                        static_cast<uint64_t>(kMessages);
        },
        8000));

    const mqttc::AsyncSnapshot snapshot = app.client->snapshot();
    CHECK_EQ(snapshot.stats.received, static_cast<uint64_t>(kMessages));
    CHECK_EQ(snapshot.stats.dropped, snapshot.dropped_inbound);  // 核心的计数与外壳一致
    CHECK_EQ(delivered.load(), static_cast<int>(snapshot.delivered));

    app.client->stop();
}

TEST(async_handler_can_publish_again_from_the_callback) {
    std::atomic<int> publish_result{999};
    mqttc::AsyncClient* client_ptr = nullptr;

    Connected app = connect_async(async_options(), [&](const mqttc::Message& message) {
        // 回调里再发一条：投递线程调 publish() 不该死锁
        publish_result = static_cast<int>(client_ptr->publish("cmd/ack", {'o', 'k'}));
        return message.topic == "cmd/x";
    });
    client_ptr = app.client.get();

    app.transport->feed(publish_bytes("cmd/x"));

    CHECK(wait_until([&] { return publish_result.load() != 999; }));
    CHECK_EQ(publish_result.load(), static_cast<int>(mqttc::Error::None));
    CHECK(wait_until([&] { return app.client->stats().published == 1; }));

    app.client->stop();
}

TEST(async_slow_handler_does_not_stall_the_io_thread) {
    mqttc::ClientOptions options = async_options();
    options.keepalive = 1;  // 1 秒
    constexpr int kMessages = 6;

    std::atomic<int> delivered{0};
    Connected app = connect_async(
        options,
        [&](const mqttc::Message&) {
            sleep_ms(500);  // 回调总耗时 ~2 秒
            ++delivered;
            return true;
        },
        5);
    app.transport->set_auto_pingresp(true);

    const uint64_t start_ms = mqttc::monotonic_ms();
    const uint64_t ticks_before = app.client->stats().ticks;
    for (int i = 0; i < kMessages; ++i) {
        app.transport->feed(publish_bytes("slow/" + std::to_string(i)));
    }

    CHECK(wait_until([&] { return app.client->snapshot().delivered == kMessages; }, 12000));

    // 回调这 3 秒里 IO 线程照样在转。真要是回调跑在 IO 线程上，这 3 秒里
    // ticks 只能涨"回调次数"那么几下（≤6）；分开线程时是几百下。阈值取 20，
    // 两边都不沾：不依赖机器快慢，只依赖"一个数量级的差距"。
    const uint64_t ticks = app.client->stats().ticks - ticks_before;
    CHECK(ticks >= 20);

    // 心跳也没被拖：keepalive 1 秒，第一个 PINGREQ 该在 1 秒出头就发出去
    const std::vector<uint64_t> pings = app.transport->pingreq_times_ms();
    REQUIRE(!pings.empty());
    CHECK(pings[0] >= start_ms + 900);
    CHECK(pings[0] <= start_ms + 1200);
    CHECK_EQ(app.client->stats().keepalive_timeouts, static_cast<uint64_t>(0));

    app.client->stop();
}

TEST(async_stop_drains_the_inbox_and_is_idempotent) {
    constexpr int kMessages = 10;
    std::atomic<int> delivered{0};
    Connected app = connect_async(async_options(), [&](const mqttc::Message&) {
        sleep_ms(20);
        ++delivered;
        return true;
    });

    for (int i = 0; i < kMessages; ++i) {
        app.transport->feed(publish_bytes("drain/" + std::to_string(i)));
    }
    CHECK(wait_until([&] { return app.client->snapshot().stats.received == kMessages; }, 3000));

    const uint64_t stop_started = mqttc::monotonic_ms();
    app.client->stop();
    const uint64_t stop_took = mqttc::monotonic_ms() - stop_started;

    CHECK(stop_took < 2000);  // 传输层的 read 是有界的，join 也就该有界
    CHECK_EQ(delivered.load(), kMessages);  // 停机时把队列排空，不丢已收到的
    CHECK_EQ(app.client->snapshot().delivered, static_cast<uint64_t>(kMessages));
    CHECK_EQ(app.client->snapshot().queued_inbound, static_cast<size_t>(0));
    CHECK(!app.client->running());

    app.client->stop();  // 幂等
}

TEST(async_handler_can_be_replaced_while_running) {
    std::atomic<int> first{0};
    std::atomic<int> second{0};
    Connected app = connect_async(async_options(), [&](const mqttc::Message&) {
        ++first;
        return true;
    });

    app.transport->feed(publish_bytes("one"));
    CHECK(wait_until([&] { return first.load() == 1; }));

    app.client->set_handler([&](const mqttc::Message&) {
        ++second;
        return true;
    });
    app.transport->feed(publish_bytes("two"));
    CHECK(wait_until([&] { return second.load() == 1; }));

    CHECK_EQ(first.load(), 1);  // 换过之后旧回调不再被调用
    app.client->stop();
}

TEST(async_returns_busy_when_the_command_queue_is_full) {
    mqttc::ClientOptions options = async_options();
    options.command_queue_capacity = 1;

    // read 阻塞久一点，让 IO 线程确实停在 read 上，命令才会堆在队列里
    Connected app = connect_async(options, mqttc::MessageHandler(), 300);
    sleep_ms(50);

    int busy = 0;
    for (int i = 0; i < 3; ++i) {
        if (app.client->publish("q/" + std::to_string(i), {'x'}) == mqttc::Error::Busy) ++busy;
    }
    CHECK(busy >= 1);  // 队列只有 1 个位置，后面几个必须被明确拒掉

    app.client->stop();
}

TEST(async_stop_races_with_publish_and_rejects_afterwards) {
    Connected app = connect_async(async_options(), mqttc::MessageHandler());

    std::atomic<bool> keep_going{true};
    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};

    std::thread publisher([&] {
        while (keep_going.load()) {
            if (app.client->publish("race/x", {'x'}) == mqttc::Error::None) {
                ++accepted;
            } else {
                ++rejected;
            }
        }
    });

    sleep_ms(30);           // 让发布线程先跑起来
    app.client->stop();     // 与 publish() 直接竞争
    keep_going = false;
    publisher.join();

    CHECK(!app.client->running());
    CHECK(accepted.load() > 0);   // 停机前确实有命令被收下
    CHECK(rejected.load() > 0);   // 停机后一律明确拒绝，不静默丢掉
    CHECK(app.client->publish("race/x", {'x'}) == mqttc::Error::Closed);
}
