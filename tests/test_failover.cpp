// 多端点故障转移测试：端点轮转顺序、切换计数、读写转发、端点串解析。
// 用测试自己的假传输（状态放在共享的 World 里），不碰真 socket。

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/failover.hpp"
#include "mqttc/transport.hpp"
#include "support.hpp"
#include "test.hpp"

namespace {

// 端点世界的全部状态；假传输对象本身无状态，随时可以被装饰器换掉
struct World {
    std::vector<bool> reachable = {true, true};
    std::vector<size_t> connect_calls = {0, 0};
    std::vector<mqttc::ByteVec> inbox = {{}, {}};
    std::vector<std::string> connected_order;  // 每次成功连上的端点标签
    mqttc::ByteVec written;
    size_t closes = 0;
};

size_t index_of(std::string_view host) {
    return host == "ep0" ? 0 : 1;
}

class WorldTransport : public mqttc::Transport {
public:
    WorldTransport(World& world, size_t index) : world_(world), index_(index) {}

    mqttc::Error connect() override {
        ++world_.connect_calls[index_];
        if (!world_.reachable[index_]) return mqttc::Error::Closed;
        world_.connected_order.push_back(index_ == 0 ? "ep0" : "ep1");
        return mqttc::Error::None;
    }

    mqttc::Result<size_t> read(uint8_t* buffer, size_t capacity) override {
        mqttc::ByteVec& inbox = world_.inbox[index_];
        if (inbox.empty()) return mqttc::Result<size_t>::failure(mqttc::Error::WouldBlock);
        const size_t count = std::min(capacity, inbox.size());
        for (size_t i = 0; i < count; ++i) buffer[i] = inbox[i];
        inbox.erase(inbox.begin(), inbox.begin() + static_cast<std::ptrdiff_t>(count));
        return mqttc::Result<size_t>::success(count);
    }

    mqttc::Error write(const uint8_t* data, size_t size) override {
        world_.written.insert(world_.written.end(), data, data + size);
        return mqttc::Error::None;
    }

    void close() override { ++world_.closes; }

private:
    World& world_;
    size_t index_;
};

std::unique_ptr<mqttc::Transport> make_world_transport(World& world,
                                                       const mqttc::Endpoint& endpoint) {
    return std::make_unique<WorldTransport>(world, index_of(endpoint.host));
}

std::vector<mqttc::Endpoint> two_endpoints() {
    return {mqttc::Endpoint{"ep0", 1883}, mqttc::Endpoint{"ep1", 1884}};
}

TEST(failover_uses_the_primary_when_it_is_up) {
    World world;
    mqttc::FailoverTransport transport(two_endpoints(),
                                       [&world](const mqttc::Endpoint& e) {
                                           return make_world_transport(world, e);
                                       });

    CHECK(transport.connect() == mqttc::Error::None);
    CHECK(transport.connected());
    CHECK_EQ(transport.switches(), static_cast<size_t>(0));
    CHECK_EQ(transport.endpoint_attempts(), static_cast<size_t>(1));
    CHECK_EQ(world.connected_order, (std::vector<std::string>{"ep0"}));
    CHECK_EQ(world.connect_calls, (std::vector<size_t>{1, 0}));
}

TEST(failover_skips_a_dead_primary_and_counts_the_switch) {
    World world;
    world.reachable = {false, true};
    mqttc::FailoverTransport transport(two_endpoints(),
                                       [&world](const mqttc::Endpoint& e) {
                                           return make_world_transport(world, e);
                                       });

    CHECK(transport.connect() == mqttc::Error::None);
    CHECK_EQ(transport.switches(), static_cast<size_t>(1));
    CHECK_EQ(transport.endpoint_attempts(), static_cast<size_t>(2));
    CHECK_EQ(world.connect_calls, (std::vector<size_t>{1, 1}));
    CHECK_EQ(transport.preferred_endpoint().host, std::string("ep1"));
}

TEST(failover_retries_the_endpoint_that_worked_first) {
    World world;
    world.reachable = {false, true};
    mqttc::FailoverTransport transport(two_endpoints(),
                                       [&world](const mqttc::Endpoint& e) {
                                           return make_world_transport(world, e);
                                       });

    CHECK(transport.connect() == mqttc::Error::None);
    CHECK(transport.connect() == mqttc::Error::None);  // Client 每次 begin_connect 都是 close + connect

    CHECK_EQ(world.connect_calls, (std::vector<size_t>{1, 2}));  // 死端点不再被撞第二次
    CHECK_EQ(transport.switches(), static_cast<size_t>(1));      // 只有第一次算切换
    CHECK_EQ(world.closes, static_cast<size_t>(1));  // 第二次 connect 前关掉了上次那条
}

TEST(failover_fails_when_every_endpoint_is_down) {
    World world;
    world.reachable = {false, false};
    mqttc::FailoverTransport transport(two_endpoints(),
                                       [&world](const mqttc::Endpoint& e) {
                                           return make_world_transport(world, e);
                                       });

    CHECK(transport.connect() != mqttc::Error::None);
    CHECK(!transport.connected());
    CHECK_EQ(transport.switches(), static_cast<size_t>(0));  // 没连上就没有切换
    CHECK_EQ(transport.endpoint_count(), static_cast<size_t>(2));
    EXPECT_ERROR(transport.read(nullptr, 0), mqttc::Error::Closed);
    CHECK(transport.write(nullptr, 0) == mqttc::Error::Closed);
}

TEST(failover_does_not_touch_a_dead_primary_after_a_new_success) {
    World world;
    world.reachable = {false, true};
    mqttc::FailoverTransport transport(two_endpoints(),
                                       [&world](const mqttc::Endpoint& e) {
                                           return make_world_transport(world, e);
                                       });
    CHECK(transport.connect() == mqttc::Error::None);

    // 主端点复活了也不该抢回来：切换是有代价的，稳定优先
    world.reachable = {true, true};
    CHECK(transport.connect() == mqttc::Error::None);
    CHECK_EQ(world.connect_calls, (std::vector<size_t>{1, 2}));
    CHECK_EQ(transport.preferred_endpoint().host, std::string("ep1"));
}

TEST(failover_forwards_read_and_write_to_the_current_endpoint) {
    World world;
    world.reachable = {false, true};
    world.inbox[1] = {0x20, 0x02, 0x00, 0x00};
    mqttc::FailoverTransport transport(two_endpoints(),
                                       [&world](const mqttc::Endpoint& e) {
                                           return make_world_transport(world, e);
                                       });
    CHECK(transport.connect() == mqttc::Error::None);

    const mqttc::ByteVec payload = {0xC0, 0x00};
    CHECK(transport.write(payload.data(), payload.size()) == mqttc::Error::None);
    CHECK_EQ(world.written, payload);

    uint8_t buffer[8] = {0};
    auto read = transport.read(buffer, sizeof(buffer));
    REQUIRE(read.ok());
    CHECK_EQ(read.value(), static_cast<size_t>(4));
    CHECK_EQ(buffer[0], static_cast<uint8_t>(0x20));
}

TEST(failover_rejects_an_empty_endpoint_list) {
    World world;
    mqttc::FailoverTransport transport({}, [&world](const mqttc::Endpoint& e) {
        return make_world_transport(world, e);
    });
    CHECK(transport.connect() == mqttc::Error::InvalidArgument);
    CHECK(!transport.connected());
}

TEST(failover_works_as_the_transport_of_a_real_client) {
    World world;
    world.reachable = {false, true};
    world.inbox[1] = support::connack(0x00);

    mqttc::FailoverTransport transport(two_endpoints(), [&world](const mqttc::Endpoint& e) {
        return make_world_transport(world, e);
    });
    mqttc::Client client(transport, support::options_with());

    CHECK(client.start(0) == mqttc::Error::None);
    client.pump(0);
    CHECK_EQ(client.state(), mqttc::State::Connected);
    CHECK_EQ(client.stats().reconnects, static_cast<uint64_t>(0));  // 首次连接不算重连
    CHECK_EQ(transport.switches(), static_cast<size_t>(1));
    CHECK_EQ(world.connected_order, (std::vector<std::string>{"ep1"}));
}

TEST(parse_endpoints_accepts_hosts_with_and_without_ports) {
    auto parsed = mqttc::parse_endpoints("127.0.0.1:1884, broker.example.net ,10.0.0.9:8883");
    REQUIRE(parsed.ok());
    const std::vector<mqttc::Endpoint>& endpoints = parsed.value();
    REQUIRE(endpoints.size() == 3);
    CHECK_EQ(endpoints[0].host, std::string("127.0.0.1"));
    CHECK_EQ(endpoints[0].port, static_cast<uint16_t>(1884));
    CHECK_EQ(endpoints[1].host, std::string("broker.example.net"));
    CHECK_EQ(endpoints[1].port, static_cast<uint16_t>(1883));  // 省了端口就用默认值
    CHECK_EQ(endpoints[2].port, static_cast<uint16_t>(8883));

    CHECK_EQ(mqttc::to_string(endpoints[2]), std::string("10.0.0.9:8883"));
}

TEST(parse_endpoints_rejects_malformed_input) {
    CHECK(mqttc::parse_endpoints("").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("   ").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("host:0").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("host:65536").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("host:99999").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("host:").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("host:abc").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("host:18 83").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints(":1883").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("a:1,,b:2").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints("a:1,").error() == mqttc::Error::InvalidArgument);
    CHECK(mqttc::parse_endpoints(",a:1").error() == mqttc::Error::InvalidArgument);

    // 自定义默认端口
    auto parsed = mqttc::parse_endpoints("host", 8883);
    REQUIRE(parsed.ok());
    CHECK_EQ(parsed.value()[0].port, static_cast<uint16_t>(8883));
}

} // namespace
