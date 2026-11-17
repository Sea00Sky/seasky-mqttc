// mqttc 命令行：sub / pub 两个子命令。
// 单线程 pump 循环，每 5–10 ms 推进一次状态机。

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/failover.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

#if MQTTC_WITH_TLS
#include "mqttc/tls.hpp"
#endif

namespace {

volatile std::sig_atomic_t g_stop = 0;

void on_interrupt(int) { g_stop = 1; }

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

struct Options {
    std::string host_text = "127.0.0.1";  // -H 的原样内容：逗号分隔的 host[:port] 表
    std::vector<mqttc::Endpoint> endpoints;
    uint16_t port = 1883;  // -p：那些没写端口的端点的默认端口
    std::string topic;
    std::string message;
    std::string client_id = "mqttc-cli";
    std::string username;
    std::string password;
    uint16_t keepalive = 60;
    uint8_t qos = 0;
    bool retain = false;
    bool message_given = false;
    int count = 0;
    mqttc::LogLevel log_level = mqttc::LogLevel::Warn;  // 默认只报问题
    bool print_stats = false;

    // TLS：默认关（和库的默认值不同：CLI 的默认端口 1883 是明文端口）。
    // 一旦给 --tls，证书校验默认是开的；--insecure 才能关。
    bool tls = false;
    std::string ca_file;
    std::string ca_path;
    std::string cert_file;
    std::string key_file;
    std::string sni;
    bool insecure = false;
    bool port_given = false;
};

void print_usage(std::FILE* out) {
    std::fputs(
        "用法:\n"
        "  mqttc pub -t <topic> -m <message> [-r] [通用选项]\n"
        "  mqttc sub -t <filter> [-c count] [通用选项]\n"
        "\n"
        "通用选项:\n"
        "  -H <hosts>       broker 地址，默认 127.0.0.1；可给多个做故障转移，\n"
        "                   如 -H mqtt1.local:1883,mqtt2.local:1883（按顺序试，\n"
        "                   连上的那个下次优先）\n"
        "  -p <port>        broker 端口，默认 1883（只作用于没写端口的地址）\n"
        "  -i <client id>   客户端标识，默认 mqttc-cli\n"
        "  -k <seconds>     keepalive 秒数，默认 60\n"
        "  -u <username>    用户名\n"
        "  -P <password>    密码（需同时给 -u）\n"
        "  -q <0|1>         QoS，默认 0\n"
        "  --log-level <级别>  error / warn / info / debug，默认 warn\n"
        "  --stats          退出时把统计（一行一个 key=value）打到 stderr\n"
        "  -h               显示本帮助\n"
        "\n"
        "TLS 选项（本构建是否带 TLS 取决于 MQTTC_ENABLE_TLS；关闭时给 --tls 会直接报错）：\n"
        "  --tls            用 TLS 连接；默认校验服务端证书\n"
        "  --cafile <path>  CA 证书文件（PEM）；与 --capath 都不给则用系统默认信任库\n"
        "  --capath <dir>   CA 证书目录\n"
        "  --cert <path>    客户端证书（服务端要求 mTLS 时用）\n"
        "  --key <path>     客户端私钥（必须与 --cert 同时给）\n"
        "  --sni <name>     SNI 与主机名校验用的名字，默认用 -H 里的 host\n"
        "  --insecure       关闭服务端证书校验：连接可被中间人冒充，只用于实验\n"
        "\n"
        "pub 专用:\n"
        "  -t <topic>       主题名（必填）\n"
        "  -m <message>     消息内容（必填）\n"
        "  -r               置 retain 位\n"
        "\n"
        "sub 专用:\n"
        "  -t <filter>      主题过滤器（必填）\n"
        "  -c <count>       收到 count 条后退出，默认一直跑（Ctrl+C 退出）\n",
        out);
}

bool parse_uint16(const char* text, uint16_t& out) {
    char* end = nullptr;
    const unsigned long value = std::strtoul(text, &end, 10);
    if (end == nullptr || *end != '\0' || value > 65535) return false;
    out = static_cast<uint16_t>(value);
    return true;
}

bool parse_int(const char* text, int& out) {
    char* end = nullptr;
    const long value = std::strtol(text, &end, 10);
    if (end == nullptr || *end != '\0' || value <= 0 || value > 1000000) return false;
    out = static_cast<int>(value);
    return true;
}

bool parse_log_level(const char* text, mqttc::LogLevel& out) {
    if (std::strcmp(text, "error") == 0) {
        out = mqttc::LogLevel::Error;
    } else if (std::strcmp(text, "warn") == 0) {
        out = mqttc::LogLevel::Warn;
    } else if (std::strcmp(text, "info") == 0) {
        out = mqttc::LogLevel::Info;
    } else if (std::strcmp(text, "debug") == 0) {
        out = mqttc::LogLevel::Debug;
    } else {
        return false;
    }
    return true;
}

// 结构化日志走 stderr，stdout 留给消息本身（管道里只取 payload 时不受影响）
mqttc::Logger make_logger(mqttc::LogLevel level) {
    return mqttc::Logger(
        [](mqttc::LogLevel event_level, const std::string& text) {
            std::fprintf(stderr, "[mqttc:%s] %s\n", mqttc::log_level_name(event_level), text.c_str());
        },
        level);
}

bool parse_args(int argc, char** argv, bool subscribe, Options& options, std::string& error) {
    for (int i = 2; i < argc; ++i) {
        const std::string arg = argv[i];

        auto next_value = [&](const char* flag) -> const char* {
            if (i + 1 >= argc) {
                error = std::string("缺少参数: ") + flag;
                return nullptr;
            }
            return argv[++i];
        };

        if (arg == "-h" || arg == "--help") {
            print_usage(stdout);
            std::exit(0);
        } else if (arg == "-H") {
            const char* value = next_value("-H");
            if (value == nullptr) return false;
            options.host_text = value;
        } else if (arg == "-p") {
            const char* value = next_value("-p");
            if (value == nullptr) return false;
            if (!parse_uint16(value, options.port)) {
                error = "端口必须是 0..65535";
                return false;
            }
            options.port_given = true;
        } else if (arg == "-i") {
            const char* value = next_value("-i");
            if (value == nullptr) return false;
            options.client_id = value;
        } else if (arg == "-k") {
            const char* value = next_value("-k");
            if (value == nullptr) return false;
            int seconds = 0;
            if (!parse_int(value, seconds)) {
                error = "keepalive 必须是正数";
                return false;
            }
            options.keepalive = static_cast<uint16_t>(seconds);
        } else if (arg == "-u") {
            const char* value = next_value("-u");
            if (value == nullptr) return false;
            options.username = value;
        } else if (arg == "-P") {
            const char* value = next_value("-P");
            if (value == nullptr) return false;
            options.password = value;
        } else if (arg == "-q") {
            const char* value = next_value("-q");
            if (value == nullptr) return false;
            if (std::strcmp(value, "0") == 0) {
                options.qos = 0;
            } else if (std::strcmp(value, "1") == 0) {
                options.qos = 1;
            } else {
                error = "只支持 QoS 0 与 1";
                return false;
            }
        } else if (arg == "-t") {
            const char* value = next_value("-t");
            if (value == nullptr) return false;
            options.topic = value;
        } else if (arg == "-m" && !subscribe) {
            const char* value = next_value("-m");
            if (value == nullptr) return false;
            options.message = value;
            options.message_given = true;
        } else if (arg == "-r" && !subscribe) {
            options.retain = true;
        } else if (arg == "--log-level") {
            const char* value = next_value("--log-level");
            if (value == nullptr) return false;
            if (!parse_log_level(value, options.log_level)) {
                error = "--log-level 只支持 error / warn / info / debug";
                return false;
            }
        } else if (arg == "--stats") {
            options.print_stats = true;
        } else if (arg == "--tls") {
            options.tls = true;
        } else if (arg == "--cafile") {
            const char* value = next_value("--cafile");
            if (value == nullptr) return false;
            options.ca_file = value;
        } else if (arg == "--capath") {
            const char* value = next_value("--capath");
            if (value == nullptr) return false;
            options.ca_path = value;
        } else if (arg == "--cert") {
            const char* value = next_value("--cert");
            if (value == nullptr) return false;
            options.cert_file = value;
        } else if (arg == "--key") {
            const char* value = next_value("--key");
            if (value == nullptr) return false;
            options.key_file = value;
        } else if (arg == "--sni") {
            const char* value = next_value("--sni");
            if (value == nullptr) return false;
            options.sni = value;
        } else if (arg == "--insecure") {
            options.insecure = true;
        } else if (arg == "-c" && subscribe) {
            const char* value = next_value("-c");
            if (value == nullptr) return false;
            if (!parse_int(value, options.count)) {
                error = "-c 必须是正数";
                return false;
            }
        } else {
            error = std::string("未知或不适用的选项: ") + arg;
            return false;
        }
    }

    auto endpoints = mqttc::parse_endpoints(options.host_text, options.port);
    if (!endpoints.ok()) {
        error = "地址不合法（应为 host[:port][,host[:port]]…）: " + options.host_text;
        return false;
    }
    options.endpoints = endpoints.take();

    // MQTT over TLS 的惯例端口是 8883；用户没显式给 -p 时跟着 --tls 走
    if (options.tls && !options.port_given) {
        auto tls_endpoints = mqttc::parse_endpoints(options.host_text, 8883);
        if (!tls_endpoints.ok()) {
            error = "地址不合法（应为 host[:port][,host[:port]]…）: " + options.host_text;
            return false;
        }
        options.endpoints = tls_endpoints.take();
    }
    return true;
}

// 端点表交给 FailoverTransport：一次 connect() 里按顺序试完所有端点。
// 每个端点自己的超时由 TcpTransport 管，所以端点多了最坏耗时是"端点个数 × 预算"，
// 这里把单端点预算收到 5 秒。
std::unique_ptr<mqttc::FailoverTransport> make_transport(const Options& options) {
    mqttc::TcpTransportOptions tcp_options;
    tcp_options.connect_timeout_ms = 5000;

#if MQTTC_WITH_TLS
    if (options.tls) {
        mqttc::TlsOptions tls_options;
        tls_options.ca_file = options.ca_file;
        tls_options.ca_path = options.ca_path;
        tls_options.cert_file = options.cert_file;
        tls_options.key_file = options.key_file;
        tls_options.server_name = options.sni;
        tls_options.verify_peer = !options.insecure;

        return std::make_unique<mqttc::FailoverTransport>(
            options.endpoints,
            [tcp_options, tls_options](const mqttc::Endpoint& endpoint) {
                return std::make_unique<mqttc::TlsTransport>(endpoint.host, endpoint.port,
                                                             tls_options, tcp_options);
            });
    }
#endif

    return std::make_unique<mqttc::FailoverTransport>(
        options.endpoints,
        [tcp_options](const mqttc::Endpoint& endpoint) -> std::unique_ptr<mqttc::Transport> {
            return std::make_unique<mqttc::TcpTransport>(endpoint.host, endpoint.port, tcp_options);
        });
}

// 永久失败的原因给人看：库里存的是稳定标签，翻译留给调用方（这里）
std::string describe_failure(const mqttc::Client& client) {
    const char* reason = client.failure_reason();
    if (reason == nullptr) return "未知原因";

    if (std::strcmp(reason, "connack_rejected") == 0) {
        const unsigned code = static_cast<unsigned>(client.failure_detail());
        const char* text = "服务端拒绝连接";
        if (code == 1) text = "协议版本不被支持";
        else if (code == 2) text = "客户端标识被拒绝";
        else if (code == 4) text = "用户名或密码错误";
        else if (code == 5) text = "未授权";
        return std::string(text) + "（CONNACK 返回码 " + std::to_string(code) + "）";
    }
    if (std::strcmp(reason, "connect_failed") == 0) {
        return std::string("连接尝试次数用尽（") + std::to_string(client.failure_detail()) + " 次）";
    }
    if (std::strcmp(reason, "cert_verify_failed") == 0) {
        return "TLS 证书校验失败（CA 不认、主机名不匹配或证书过期）";
    }
    return std::string(reason) + "（detail=" + std::to_string(client.failure_detail()) + "）";
}

mqttc::ClientOptions make_client_options(const Options& options) {
    mqttc::ClientOptions client_options;
    client_options.client_id = options.client_id;
    client_options.keepalive = options.keepalive;
    if (!options.username.empty()) client_options.username = options.username;
    if (!options.password.empty()) client_options.password = options.password;
    client_options.logger = make_logger(options.log_level);
    return client_options;
}

mqttc::QoS qos_of(const Options& options) {
    return options.qos == 1 ? mqttc::QoS::AtLeastOnce : mqttc::QoS::AtMostOnce;
}

bool wait_connected(mqttc::Client& client, uint32_t timeout_ms) {
    const uint64_t deadline = mqttc::monotonic_ms() + timeout_ms;
    while (client.state() != mqttc::State::Connected) {
        if (client.state() == mqttc::State::Failed) return false;  // 再等也没用
        if (mqttc::monotonic_ms() >= deadline) return false;
        client.pump(mqttc::monotonic_ms());
        sleep_ms(5);
    }
    return true;
}

void print_stats_if_requested(const Options& options, const mqttc::Client& client) {
    if (!options.print_stats) return;
    std::fputs(mqttc::format_stats(client.stats()).c_str(), stderr);
}

// 永久失败（凭据错、标识被拒、证书校验失败、试满次数）用 3 退出，
// 和"暂时连不上"的 2 分开。传输层的 last_error() 带上具体原因（TLS 尤其需要）。
int report_connect_failure(const Options& options, const mqttc::Client& client,
                           const mqttc::Transport& transport) {
    const std::string detail = transport.last_error();
    if (client.state() == mqttc::State::Failed) {
        std::fprintf(stderr, "永久失败，不再重试: %s%s%s\n", describe_failure(client).c_str(),
                     detail.empty() ? "" : "；", detail.c_str());
        return 3;
    }
    std::fprintf(stderr, "连接超时: %s%s%s\n", options.host_text.c_str(),
                 detail.empty() ? "" : "；", detail.c_str());
    return 2;
}

int run_publish(const Options& options) {
    auto transport = make_transport(options);
    mqttc::Client client(*transport, make_client_options(options));

    client.start(mqttc::monotonic_ms());
    if (!wait_connected(client, 5000)) {
        const int code = report_connect_failure(options, client, *transport);
        client.stop();
        return code;
    }
    std::fprintf(stderr, "已连接 %s%s\n",
                 mqttc::to_string(transport->preferred_endpoint()).c_str(),
                 options.tls ? "（TLS）" : "");

    const mqttc::QoS qos = qos_of(options);
    mqttc::ByteVec payload(options.message.begin(), options.message.end());
    if (client.publish(options.topic, std::move(payload), qos, options.retain) != mqttc::Error::None) {
        std::fprintf(stderr, "发布失败\n");
        client.stop();
        return 2;
    }

    if (qos == mqttc::QoS::AtLeastOnce) {
        const uint64_t deadline = mqttc::monotonic_ms() + 5000;
        while (client.inflight_count() > 0 && mqttc::monotonic_ms() < deadline) {
            client.pump(mqttc::monotonic_ms());
            sleep_ms(5);
        }
        if (client.inflight_count() > 0) {
            std::fprintf(stderr, "没有等到 PUBACK\n");
            client.stop();
            return 2;
        }
    }

    client.stop();
    std::fprintf(stderr, "已发布到 %s（%u 字节，QoS %u）\n", options.topic.c_str(),
                 static_cast<unsigned>(options.message.size()), static_cast<unsigned>(options.qos));
    print_stats_if_requested(options, client);
    return 0;
}

int run_subscribe(const Options& options) {
    auto transport = make_transport(options);

    int received = 0;
    mqttc::Client client(
        *transport, make_client_options(options), [&received](const mqttc::Message& message) {
            std::fwrite(message.topic.data(), 1, message.topic.size(), stdout);
            std::fputc(' ', stdout);
            std::fwrite(message.payload.data(), 1, message.payload.size(), stdout);
            std::fputc('\n', stdout);
            std::fflush(stdout);
            ++received;
            return true;
        });

    if (client.subscribe(options.topic, qos_of(options)) != mqttc::Error::None) {
        std::fprintf(stderr, "订阅失败：主题过滤器非法: %s\n", options.topic.c_str());
        return 1;
    }

    client.start(mqttc::monotonic_ms());
    if (!wait_connected(client, 5000)) {
        const int code = report_connect_failure(options, client, *transport);
        client.stop();
        return code;
    }
    std::fprintf(stderr, "已连接 %s%s，订阅 %s（QoS %u）\n",
                 mqttc::to_string(transport->preferred_endpoint()).c_str(),
                 options.tls ? "（TLS）" : "", options.topic.c_str(),
                 static_cast<unsigned>(options.qos));

    uint64_t reconnects_seen = 0;
    size_t switches_seen = 0;
    while (g_stop == 0 && (options.count == 0 || received < options.count)) {
        client.pump(mqttc::monotonic_ms());
        if (client.state() == mqttc::State::Failed) {
            std::fprintf(stderr, "永久失败，不再重试: %s\n", describe_failure(client).c_str());
            client.stop();
            print_stats_if_requested(options, client);
            return 3;
        }
        if (client.stats().reconnects > reconnects_seen) {
            reconnects_seen = client.stats().reconnects;
            std::fprintf(stderr, "[mqttc] 已重连（第 %llu 次）\n",
                         static_cast<unsigned long long>(reconnects_seen));
        }
        if (transport->switches() > switches_seen) {
            switches_seen = transport->switches();
            std::fprintf(stderr, "[mqttc] 已切换到备用端点 %s（第 %llu 次）\n",
                         mqttc::to_string(transport->preferred_endpoint()).c_str(),
                         static_cast<unsigned long long>(switches_seen));
        }
        sleep_ms(10);
    }

    client.stop();
    print_stats_if_requested(options, client);
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        print_usage(stderr);
        return 1;
    }

    const std::string command = argv[1];
    if (command == "-h" || command == "--help" || command == "help") {
        print_usage(stdout);
        return 0;
    }
    const bool subscribe = command == "sub";
    if (!subscribe && command != "pub") {
        std::fprintf(stderr, "未知子命令: %s\n\n", command.c_str());
        print_usage(stderr);
        return 1;
    }

    Options options;
    std::string error;
    if (!parse_args(argc, argv, subscribe, options, error)) {
        std::fprintf(stderr, "参数错误: %s\n\n", error.c_str());
        print_usage(stderr);
        return 1;
    }
    if (options.topic.empty()) {
        std::fprintf(stderr, "必须用 -t 指定主题\n");
        return 1;
    }
    if (!subscribe && !options.message_given) {
        std::fprintf(stderr, "pub 必须用 -m 指定消息内容\n");
        return 1;
    }

#if !MQTTC_WITH_TLS
    if (options.tls) {
        std::fprintf(stderr, "这个构建没有 TLS（MQTTC_ENABLE_TLS=OFF），不支持 --tls\n");
        return 1;
    }
#endif
    if (!options.tls && (options.insecure || !options.ca_file.empty() || !options.ca_path.empty() ||
                         !options.cert_file.empty() || !options.key_file.empty() ||
                         !options.sni.empty())) {
        std::fprintf(stderr, "TLS 相关选项（--cafile/--capath/--cert/--key/--sni/--insecure）"
                             "必须与 --tls 一起用\n");
        return 1;
    }
    if (options.insecure) {
        std::fprintf(stderr, "[mqttc] 警告：--insecure 关闭了服务端证书校验，连接可能被中间人冒充\n");
    }

    std::signal(SIGINT, on_interrupt);
    return subscribe ? run_subscribe(options) : run_publish(options);
}