#pragma once

// 可观测性的最小件：日志接口 + 结构化一行。
//
// 设计约束：
// - 库里**不打印任何东西**：没人装 sink 就是纯空转，不碰 stdout/stderr、不碰全局状态。
// - 不引第三方、不用宏：调用方自己决定 sink（stderr、文件、采集器都行）。
// - 只记"状态变化与异常"（连上、被拒、重连、协议错误、丢弃），热路径不记。

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace mqttc {

enum class LogLevel : uint8_t {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
};

const char* log_level_name(LogLevel level);

class Logger {
public:
    using Sink = std::function<void(LogLevel, const std::string&)>;

    Logger() = default;
    Logger(Sink sink, LogLevel level) : sink_(std::move(sink)), level_(level) {}

    bool enabled(LogLevel level) const { return static_cast<bool>(sink_) && level <= level_; }

    void log(LogLevel level, const std::string& message) const {
        if (!enabled(level)) return;
        sink_(level, message);
    }

    const Sink& sink() const { return sink_; }
    LogLevel level() const { return level_; }

private:
    Sink sink_;
    LogLevel level_ = LogLevel::Info;
};

// 结构化一行：`事件 key=value key=value`，值里有空格或引号才加引号。
// 用法：LogLine("connected").field("host", host).field("attempts", attempts_).str()
class LogLine {
public:
    explicit LogLine(std::string_view event) : text_(event) {}

    LogLine& field(std::string_view key, std::string_view value);

    // 必须单独给 const char* 一个重载：字符串字面量到 bool 是标准转换，
    // 到 string_view 是用户自定义转换，少了这条就会把 "abc" 记成 true
    LogLine& field(std::string_view key, const char* value) {
        return field(key, std::string_view(value == nullptr ? "" : value));
    }

    LogLine& field(std::string_view key, bool value) {
        return field(key, std::string_view(value ? "true" : "false"));
    }

    // 所有整数宽度都走这一个模板：不然 uint16_t/uint32_t 在 uint64_t / int64_t / bool
    // 三个重载之间是二义的
    template <typename T, typename std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>, int> = 0>
    LogLine& field(std::string_view key, T value) {
        const std::string text = std::to_string(value);
        return field(key, std::string_view(text));
    }

    const std::string& str() const { return text_; }
    std::string take() { return std::move(text_); }

private:
    std::string text_;
};

} // namespace mqttc
