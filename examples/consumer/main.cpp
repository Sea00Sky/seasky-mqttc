// 消费方示例：证明"装出来的 mqttc"能被 find_package 找到、编译、链接、跑通。
//
//   ./consumer [host] [port]
//
// 它订阅 example/#，往 example/hello 发一条 QoS1 消息，等这条消息从 broker 转回来就退出。
// 本地看效果：先 `python e2e/mock_broker.py --port 18831`，再跑 `./consumer 127.0.0.1 18831`。

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "mqttc/client.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

int main(int argc, char** argv) {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const uint16_t port = argc > 2 ? static_cast<uint16_t>(std::atoi(argv[2])) : 1883;

    bool got_message = false;

    mqttc::ClientOptions options;
    options.client_id = "mqttc-consumer-example";
    options.keepalive = 5;

    mqttc::TcpTransport transport(host, port);
    mqttc::Client client(transport, options, [&got_message](const mqttc::Message& message) {
        std::printf("收到 %s: %.*s\n", message.topic.c_str(),
                    static_cast<int>(message.payload.size()),
                    reinterpret_cast<const char*>(message.payload.data()));
        got_message = true;
        return true;
    });

    const uint64_t started = mqttc::monotonic_ms();
    const auto deadline_ms = [started](uint64_t budget) { return started + budget; };

    client.start(mqttc::monotonic_ms());
    while (client.state() != mqttc::State::Connected &&
           mqttc::monotonic_ms() < deadline_ms(5000)) {
        client.pump(mqttc::monotonic_ms());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (client.state() != mqttc::State::Connected) {
        std::fprintf(stderr, "连不上 %s:%u\n", host.c_str(), port);
        return 1;
    }

    if (client.subscribe("example/#", mqttc::QoS::AtLeastOnce) != mqttc::Error::None) {
        std::fprintf(stderr, "订阅失败\n");
        return 1;
    }

    const mqttc::ByteVec payload = {'h', 'e', 'l', 'l', 'o'};
    if (client.publish("example/hello", payload, mqttc::QoS::AtLeastOnce) != mqttc::Error::None) {
        std::fprintf(stderr, "发布失败\n");
        return 1;
    }

    while (!got_message && mqttc::monotonic_ms() < deadline_ms(8000)) {
        client.pump(mqttc::monotonic_ms());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    client.stop();

    if (!got_message) {
        std::fprintf(stderr, "没等到自己发出去的那条消息\n");
        return 1;
    }
    std::printf("OK\n");
    return 0;
}
