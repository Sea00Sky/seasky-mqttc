// 重连策略测试：退避加倍与上限、抖动上下界与可复现性、失败分类、尝试次数上限。
// 全部走 InMemoryTransport + 注入时间，不 sleep、不碰 socket。

#include <cstdint>
#include <string>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"
#include "support.hpp"
#include "test.hpp"

namespace {

// 抖动测试用：退避上下限相同 => 基线恒为 1000，抖动是唯一变量
mqttc::ClientOptions jitter_options(double ratio, uint64_t seed) {
    mqttc::ClientOptions options;
    options.client_id = "tester";
    options.keepalive = 0;  // 这个文件只关心重连定时器，不要心跳来搅局
    options.reconnect_min_ms = 1000;
    options.reconnect_max_ms = 1000;
    options.backoff_jitter_ratio = ratio;
    options.rng_seed = seed;
    return options;
}

mqttc::ClientOptions backoff_options() {
    mqttc::ClientOptions options;
    options.client_id = "tester";
    options.keepalive = 0;
    options.reconnect_min_ms = 100;
    options.reconnect_max_ms = 400;
    options.backoff_jitter_ratio = 0.0;  // 下面断言的是精确值，先关抖动
    return options;
}

// 每次把时间推到"排定的重连时刻"触发一次注定失败的尝试，取回这次排定的等待长度。
// 返回的每个值 = 该次失败后 next_attempt_ms() - 触发时刻。
std::vector<uint64_t> collect_backoff(mqttc::Client& client, size_t count) {
    std::vector<uint64_t> delays;
    delays.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const uint64_t now = client.next_attempt_ms();
        client.pump(now);
        delays.push_back(client.next_attempt_ms() - now);
    }
    return delays;
}

TEST(reconnect_backoff_doubles_up_to_the_cap) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, backoff_options());
    transport.set_connect_fails(true);

    CHECK(client.start(0) == mqttc::Error::Closed);
    CHECK_EQ(client.next_attempt_ms(), static_cast<uint64_t>(100));  // 第一次 = 下限

    const std::vector<uint64_t> delays = collect_backoff(client, 6);
    const std::vector<uint64_t> expected = {200, 400, 400, 400, 400, 400};  // 加倍到上限后不再涨
    CHECK_EQ(delays, expected);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(7));  // 首次 + 6 次重试
}

TEST(reconnect_backoff_resets_after_a_successful_connack) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, backoff_options());
    transport.set_connect_fails(true);

    CHECK(client.start(0) == mqttc::Error::Closed);
    collect_backoff(client, 3);  // 退避爬到上限 400

    // 网络恢复：下一次尝试连上并拿到 CONNACK
    transport.set_connect_fails(false);
    uint64_t now = client.next_attempt_ms();
    client.pump(now);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);
    transport.feed(support::connack(0x00));
    client.pump(now);
    CHECK_EQ(client.state(), mqttc::State::Connected);

    // 再掉一次线：等的是下限，不是上次爬到的上限
    transport.set_closed(true);
    client.pump(now + 1);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(client.next_attempt_ms() - (now + 1), static_cast<uint64_t>(100));
}

TEST(reconnect_jitter_disabled_waits_exactly_the_base) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, jitter_options(0.0, 12345));
    transport.set_connect_fails(true);

    CHECK(client.start(0) == mqttc::Error::Closed);
    for (uint64_t delay : collect_backoff(client, 8)) {
        CHECK_EQ(delay, static_cast<uint64_t>(1000));
    }
}

TEST(reconnect_jitter_stays_within_bounds_and_actually_varies) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, jitter_options(0.5, 42));
    transport.set_connect_fails(true);

    CHECK(client.start(0) == mqttc::Error::Closed);

    bool varies = false;
    for (uint64_t delay : collect_backoff(client, 64)) {
        CHECK(delay >= 500 && delay <= 1500);  // 基线 1000 ± 50%
        if (delay != 1000) varies = true;
    }
    CHECK(varies);  // 抖动得真的在动，不是"设了参数但没用上"
}

TEST(reconnect_jitter_is_reproducible_for_a_given_seed) {
    mqttc::InMemoryTransport first_transport;
    mqttc::Client first(first_transport, jitter_options(0.3, 2024));
    first_transport.set_connect_fails(true);
    CHECK(first.start(0) == mqttc::Error::Closed);
    const std::vector<uint64_t> first_delays = collect_backoff(first, 32);

    mqttc::InMemoryTransport second_transport;
    mqttc::Client second(second_transport, jitter_options(0.3, 2024));
    second_transport.set_connect_fails(true);
    CHECK(second.start(0) == mqttc::Error::Closed);
    const std::vector<uint64_t> second_delays = collect_backoff(second, 32);

    CHECK_EQ(first_delays, second_delays);  // 同种子 => 同序列：抖动不制造偶发失败
}

TEST(reconnect_jitter_auto_seeded_is_still_bounded) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, jitter_options(0.3, 0));  // 0 = 自动播种
    transport.set_connect_fails(true);

    CHECK(client.start(0) == mqttc::Error::Closed);
    for (uint64_t delay : collect_backoff(client, 16)) {
        CHECK(delay >= 700 && delay <= 1300);
    }
}

TEST(reconnect_gives_up_after_max_connect_attempts) {
    mqttc::ClientOptions options = backoff_options();
    options.max_connect_attempts = 3;
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);
    transport.set_connect_fails(true);

    CHECK(client.start(0) == mqttc::Error::Closed);  // 第 1 次尝试
    CHECK_EQ(client.state(), mqttc::State::Backoff);

    client.pump(client.next_attempt_ms());  // 第 2 次尝试
    CHECK_EQ(client.state(), mqttc::State::Backoff);

    client.pump(client.next_attempt_ms());  // 第 3 次尝试，配额用完
    CHECK_EQ(client.state(), mqttc::State::Failed);
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(1));
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(3));

    // 时间再怎么走都不再连
    const uint64_t now = client.next_attempt_ms();
    for (uint64_t later = now; later <= now + 100000; later += 1000) {
        client.pump(later);
    }
    CHECK_EQ(transport.connect_calls(), static_cast<size_t>(3));
}

TEST(reconnect_failed_client_can_be_started_again) {
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, backoff_options());

    client.start(0);
    transport.feed(support::connack(4));  // 凭据错 => 永久失败
    client.pump(0);
    CHECK_EQ(client.state(), mqttc::State::Failed);

    // start() 清零计数与退避：调用方修好凭据后能再来一轮
    CHECK(client.start(5000) == mqttc::Error::None);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);
    transport.feed(support::connack(0x00));
    client.pump(5000);
    CHECK_EQ(client.state(), mqttc::State::Connected);
}

TEST(reconnect_after_keepalive_timeout_resends_inflight) {
    mqttc::ClientOptions options = backoff_options();
    options.keepalive = 2;  // 2000 ms
    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);

    client.start(0);
    transport.feed(support::connack(0x00));
    client.pump(0);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    CHECK(client.publish("a/b", {1, 2, 3}, mqttc::QoS::AtLeastOnce) == mqttc::Error::None);
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(1));

    // 静默到 keepalive 到点：发 PINGREQ
    transport.clear_written();
    client.pump(2000);
    CHECK_EQ(transport.written(), (mqttc::ByteVec{0xC0, 0x00}));

    // 再等一个 keepalive 还没 PINGRESP：判链路已死，进退避
    client.pump(4000);
    CHECK_EQ(client.stats().keepalive_timeouts, static_cast<uint64_t>(1));
    CHECK_EQ(client.state(), mqttc::State::Backoff);

    // 重连成功后把在途的 QoS1 消息重发（带 DUP）
    const uint64_t retry_ms = client.next_attempt_ms();
    client.pump(retry_ms);
    CHECK_EQ(client.state(), mqttc::State::WaitingConnAck);
    transport.feed(support::connack(0x00));
    client.pump(retry_ms);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    CHECK_EQ(client.stats().resends, static_cast<uint64_t>(1));
    CHECK_EQ(client.inflight_count(), static_cast<size_t>(1));  // 还没 PUBACK，继续挂着
}

// 只在 connect() 阶段返回指定错误的传输层：用来验证 TLS 证书失败的错误分类，
// 不需要真的 OpenSSL / socket。
class FailingTransport : public mqttc::Transport {
public:
    explicit FailingTransport(mqttc::Error error) : error_(error) {}

    mqttc::Error connect() override {
        ++calls_;
        return error_;
    }
    mqttc::Result<size_t> read(uint8_t*, size_t) override {
        return mqttc::Result<size_t>::failure(mqttc::Error::Closed);
    }
    mqttc::Error write(const uint8_t*, size_t) override { return mqttc::Error::Closed; }
    void close() override {}

    size_t calls() const { return calls_; }

private:
    mqttc::Error error_;
    size_t calls_ = 0;
};

TEST(reconnect_cert_verify_failure_is_permanent_by_default) {
    FailingTransport transport(mqttc::Error::CertVerify);
    mqttc::Client client(transport, backoff_options());

    CHECK(client.start(0) == mqttc::Error::Closed);
    CHECK_EQ(client.state(), mqttc::State::Failed);
    CHECK_EQ(std::string(client.failure_reason()), std::string("cert_verify_failed"));
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(1));

    // 再推 10 秒也不会再试：同样的 CA/证书再试多少次都是同样的结果
    client.pump(10000);
    CHECK_EQ(transport.calls(), static_cast<size_t>(1));
    CHECK_EQ(client.state(), mqttc::State::Failed);
}

TEST(reconnect_cert_verify_failure_can_be_retried_when_asked) {
    FailingTransport transport(mqttc::Error::CertVerify);
    mqttc::ClientOptions options = backoff_options();
    options.retry_on_permanent = true;
    mqttc::Client client(transport, options);

    CHECK(client.start(0) == mqttc::Error::Closed);
    CHECK_EQ(client.state(), mqttc::State::Backoff);
    CHECK_EQ(client.stats().permanent_failures, static_cast<uint64_t>(0));
    CHECK_EQ(client.next_attempt_ms(), static_cast<uint64_t>(100));

    client.pump(client.next_attempt_ms());
    CHECK_EQ(transport.calls(), static_cast<size_t>(2));
}

} // namespace
