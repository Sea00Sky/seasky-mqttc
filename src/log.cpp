#include "mqttc/log.hpp"

#include "mqttc/types.hpp"  // format_stats 要用 Stats

namespace mqttc {
namespace {

bool needs_quotes(std::string_view value) {
    if (value.empty()) return true;
    for (char c : value) {
        if (c == ' ' || c == '"' || c == '=' || c == '\\') return true;
    }
    return false;
}

} // namespace

const char* log_level_name(LogLevel level) {
    switch (level) {
    case LogLevel::Error:
        return "error";
    case LogLevel::Warn:
        return "warn";
    case LogLevel::Info:
        return "info";
    case LogLevel::Debug:
        return "debug";
    }
    return "unknown";
}

LogLine& LogLine::field(std::string_view key, std::string_view value) {
    text_ += ' ';
    text_ += key;
    text_ += '=';
    if (needs_quotes(value)) {
        text_ += '"';
        for (char c : value) {
            if (c == '"' || c == '\\') text_ += '\\';
            text_ += c;
        }
        text_ += '"';
    } else {
        text_ += value;
    }
    return *this;
}

// 稳定的纯文本快照：一行一个 key=value，方便 grep，也方便以后接采集。
// 加字段是兼容的，删/改字段名不兼容。
std::string format_stats(const Stats& stats) {
    std::string out;
    const auto add = [&out](std::string_view key, uint64_t value) {
        out += key;
        out += '=';
        out += std::to_string(value);
        out += '\n';
    };

    add("published", stats.published);
    add("received", stats.received);
    add("dropped", stats.dropped);
    add("resends", stats.resends);
    add("reconnects", stats.reconnects);
    add("protocol_errors", stats.protocol_errors);
    add("keepalive_timeouts", stats.keepalive_timeouts);
    add("subscribe_failures", stats.subscribe_failures);
    add("unexpected_packets", stats.unexpected_packets);
    add("oversize_packets", stats.oversize_packets);
    add("bytes_in", stats.bytes_in);
    add("bytes_out", stats.bytes_out);
    add("ticks", stats.ticks);
    return out;
}

} // namespace mqttc
