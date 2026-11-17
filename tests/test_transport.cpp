// TcpTransport 的行为测试：只覆盖不依赖真实 broker 的部分——超时预算与错误契约。
// "连得上、能收发"由 e2e（真 socket + mock broker）覆盖，见 e2e/run_e2e.py。

#include "mqttc/transport.hpp"

#include "test.hpp"

#include <cstdint>

namespace {

// RFC 5737 的 TEST-NET-1：保留给文档用的地址，正常网络里不可达，
// 对它 connect 会一直挂着——正好用来验证"超时预算真的生效"。
constexpr const char* kBlackHoleHost = "192.0.2.1";
constexpr uint16_t kBlackHolePort = 9;

} // namespace

TEST(tcp_transport_unconnected_read_write_report_closed) {
    mqttc::TcpTransport transport("127.0.0.1", 1);

    uint8_t buffer[8] = {};
    EXPECT_ERROR(transport.read(buffer, sizeof(buffer)), mqttc::Error::Closed);

    const uint8_t data[1] = {0x01};
    CHECK_EQ(transport.write(data, sizeof(data)), mqttc::Error::Closed);

    transport.close();  // 没连过也能关
    transport.close();  // 而且可以重复关
}

TEST(tcp_transport_connect_stays_within_timeout_budget) {
    mqttc::TcpTransportOptions options;
    options.connect_timeout_ms = 300;

    mqttc::TcpTransport transport(kBlackHoleHost, kBlackHolePort, options);

    const uint64_t start = mqttc::monotonic_ms();
    const mqttc::Error error = transport.connect();
    const uint64_t cost = mqttc::monotonic_ms() - start;

    CHECK(error != mqttc::Error::None);  // 保留地址不该连上
    CHECK(cost < 3000);                  // 关键词：不能无限挂住调用线程
}

TEST(tcp_transport_connect_to_unresolvable_host_fails_bounded) {
    mqttc::TcpTransportOptions options;
    options.connect_timeout_ms = 1000;

    mqttc::TcpTransport transport("mqttc-does-not-exist.invalid", 1883, options);

    const uint64_t start = mqttc::monotonic_ms();
    const mqttc::Error error = transport.connect();
    const uint64_t cost = mqttc::monotonic_ms() - start;

    CHECK(error != mqttc::Error::None);
    CHECK(cost < 5000);
}

TEST(tcp_transport_can_retry_connect_after_failure) {
    mqttc::TcpTransportOptions options;
    options.connect_timeout_ms = 200;

    mqttc::TcpTransport transport(kBlackHoleHost, kBlackHolePort, options);

    CHECK(transport.connect() != mqttc::Error::None);
    transport.close();
    CHECK(transport.connect() != mqttc::Error::None);  // 失败后还能再来一次，不会崩
    transport.close();
}
