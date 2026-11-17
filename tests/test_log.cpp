// 可观测性：结构化日志的格式、默认静默、以及状态机真的在关键处记账。

#include <string>
#include <vector>

#include "mqttc/client.hpp"
#include "mqttc/log.hpp"
#include "mqttc/transport.hpp"
#include "mqttc/types.hpp"

#include "support.hpp"
#include "test.hpp"

TEST(log_line_is_key_value) {
    const std::string line =
        mqttc::LogLine("connected").field("attempt", 3).field("ok", true).field("client_id", "cid").str();
    CHECK_EQ(line, std::string("connected attempt=3 ok=true client_id=cid"));
}

TEST(log_line_quotes_values_with_spaces) {
    CHECK_EQ(mqttc::LogLine("e").field("v", "a b").str(), std::string("e v=\"a b\""));
    CHECK_EQ(mqttc::LogLine("e").field("v", "a\"b").str(), std::string("e v=\"a\\\"b\""));
    CHECK_EQ(mqttc::LogLine("e").field("v", "a=b").str(), std::string("e v=\"a=b\""));
    CHECK_EQ(mqttc::LogLine("e").field("v", "").str(), std::string("e v=\"\""));
}

TEST(log_line_accepts_narrow_integers) {
    const uint8_t small = 7;
    const uint32_t medium = 4096;
    const std::string line = mqttc::LogLine("e").field("a", small).field("b", medium).str();
    CHECK_EQ(line, std::string("e a=7 b=4096"));
}

TEST(logger_is_silent_without_sink) {
    mqttc::Logger logger;  // 默认没 sink
    CHECK(!logger.enabled(mqttc::LogLevel::Error));
    logger.log(mqttc::LogLevel::Error, "不该被看到");
}

TEST(logger_respects_level) {
    std::vector<std::string> lines;
    mqttc::Logger logger(
        [&lines](mqttc::LogLevel, const std::string& text) { lines.push_back(text); },
        mqttc::LogLevel::Warn);

    logger.log(mqttc::LogLevel::Info, "info");
    logger.log(mqttc::LogLevel::Debug, "debug");
    logger.log(mqttc::LogLevel::Warn, "warn");
    logger.log(mqttc::LogLevel::Error, "error");

    CHECK_EQ(lines.size(), static_cast<size_t>(2));
    CHECK_EQ(lines[0], std::string("warn"));
    CHECK_EQ(lines[1], std::string("error"));
}

TEST(log_level_names_are_stable) {
    CHECK_EQ(std::string(mqttc::log_level_name(mqttc::LogLevel::Error)), std::string("error"));
    CHECK_EQ(std::string(mqttc::log_level_name(mqttc::LogLevel::Warn)), std::string("warn"));
    CHECK_EQ(std::string(mqttc::log_level_name(mqttc::LogLevel::Info)), std::string("info"));
    CHECK_EQ(std::string(mqttc::log_level_name(mqttc::LogLevel::Debug)), std::string("debug"));
}

TEST(format_stats_covers_every_counter) {
    mqttc::Stats stats;
    stats.published = 7;
    stats.received = 8;
    stats.dropped = 9;
    stats.resends = 10;
    stats.reconnects = 11;
    stats.protocol_errors = 12;
    stats.keepalive_timeouts = 13;
    stats.subscribe_failures = 14;
    stats.unexpected_packets = 15;
    stats.oversize_packets = 16;
    stats.bytes_in = 17;
    stats.bytes_out = 18;
    stats.ticks = 19;

    const std::string text = mqttc::format_stats(stats);
    for (const char* key : {"published", "received", "dropped", "resends", "reconnects",
                            "protocol_errors", "keepalive_timeouts", "subscribe_failures",
                            "unexpected_packets", "oversize_packets", "bytes_in", "bytes_out",
                            "ticks"}) {
        CHECK(text.find(std::string(key) + "=") != std::string::npos);
    }
    CHECK(text.find("published=7\n") != std::string::npos);
    CHECK(text.find("ticks=19\n") != std::string::npos);
    CHECK_EQ(text.back(), '\n');
}

TEST(client_logs_connect_and_callers_can_count_ticks) {
    std::vector<std::string> lines;
    mqttc::ClientOptions options = support::options_with();
    options.logger = mqttc::Logger(
        [&lines](mqttc::LogLevel, const std::string& text) { lines.push_back(text); },
        mqttc::LogLevel::Debug);

    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);
    support::connect(transport, client, /*session_present=*/false, /*now_ms=*/50);

    bool saw_connected = false;
    for (const std::string& line : lines) {
        if (line.rfind("connected", 0) == 0) saw_connected = true;
    }
    CHECK(saw_connected);
    CHECK(client.stats().ticks >= 2);  // start() + 这次 pump
}

TEST(client_logs_reconnect_reason) {
    std::vector<std::string> lines;
    mqttc::ClientOptions options = support::options_with();
    options.logger = mqttc::Logger(
        [&lines](mqttc::LogLevel, const std::string& text) { lines.push_back(text); },
        mqttc::LogLevel::Debug);

    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options);
    support::connect(transport, client);

    // 长度字段 5 个字节 → 畸形报文 → 重连，日志里应该有原因
    mqttc::ByteVec bad;
    bad.push_back(0x30);
    bad.insert(bad.end(), {0xFF, 0xFF, 0xFF, 0xFF, 0x7F});
    transport.feed(bad);
    client.pump(0);

    bool saw_reason = false;
    for (const std::string& line : lines) {
        if (line.rfind("reconnect_scheduled", 0) == 0) {
            CHECK(line.find("reason=malformed_length") != std::string::npos);
            saw_reason = true;
        }
    }
    CHECK(saw_reason);
}

TEST(client_logs_dropped_messages_at_debug) {
    std::vector<std::string> lines;
    mqttc::ClientOptions options = support::options_with();
    options.logger = mqttc::Logger(
        [&lines](mqttc::LogLevel, const std::string& text) { lines.push_back(text); },
        mqttc::LogLevel::Debug);

    mqttc::InMemoryTransport transport;
    mqttc::Client client(transport, options, [](const mqttc::Message&) { return false; });
    support::connect(transport, client);

    mqttc::Publish publish;
    publish.topic = "a/b";
    publish.payload = {'x'};
    transport.feed(support::bytes_of(publish));
    client.pump(0);

    CHECK_EQ(client.stats().dropped, static_cast<uint64_t>(1));
    bool saw_drop = false;
    for (const std::string& line : lines) {
        if (line.rfind("message_dropped", 0) == 0) saw_drop = true;
    }
    CHECK(saw_drop);
}
