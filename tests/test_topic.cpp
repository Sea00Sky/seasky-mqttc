#include <string>

#include "mqttc/topic.hpp"
#include "test.hpp"

namespace {

struct MatchCase {
    const char* filter;
    const char* topic;
    bool expected;
};

void check_name(const std::string& name, bool expected) {
    if (mqttc::valid_topic_name(name) != expected) {
        t::report(__FILE__, __LINE__,
                  std::string("valid_topic_name(\"") + name + "\") != " + (expected ? "true" : "false"));
    }
}

void check_filter(const std::string& filter, bool expected) {
    if (mqttc::valid_topic_filter(filter) != expected) {
        t::report(__FILE__, __LINE__, std::string("valid_topic_filter(\"") + filter + "\") != " +
                                        (expected ? "true" : "false"));
    }
}

void check_match(const char* filter, const char* topic, bool expected) {
    if (mqttc::topic_matches(filter, topic) != expected) {
        t::report(__FILE__, __LINE__, std::string("topic_matches(\"") + filter + "\", \"" + topic +
                                        "\") != " + (expected ? "true" : "false"));
    }
}

} // namespace

TEST(topic_valid_names) {
    check_name("a", true);
    check_name("a/b", true);
    check_name("/", true);
    check_name("a//b", true);
    check_name("$SYS/broker/uptime", true);
    check_name("/finance", true);
    check_name("\xE4\xB8\xAD\xE6\x96\x87", true);  // "中文"
    check_name(std::string(65535, 'a'), true);     // 长度上限
}

TEST(topic_invalid_names) {
    check_name("", false);
    check_name("a/+/b", false);
    check_name("a/#", false);
    check_name("#", false);
    check_name("+", false);
    check_name(std::string(65536, 'a'), false);       // 超长
    check_name(std::string("a\x00b", 3), false);      // 含 U+0000
    check_name(std::string("\xC0\xAF", 2), false);    // 非法 UTF-8
    check_name(std::string("a\xFF", 2), false);       // 非法 UTF-8
}

TEST(topic_valid_filters) {
    check_filter("#", true);
    check_filter("+", true);
    check_filter("a/#", true);
    check_filter("a/+", true);
    check_filter("/#", true);
    check_filter("+/+", true);
    check_filter("a/+/#", true);
    check_filter("+//b", true);
    check_filter("sport/tennis/#", true);
    check_filter(std::string(65535, 'a'), true);
}

TEST(topic_invalid_filters) {
    check_filter("", false);
    check_filter("a/#/b", false);   // '#' 不在最后一层
    check_filter("a/#/", false);
    check_filter("#/a", false);
    check_filter("a#", false);      // '#' 不独占一层
    check_filter("#a", false);
    check_filter("sport+", false);  // '+' 不独占一层
    check_filter("+a", false);
    check_filter("a/b+", false);
    check_filter(std::string(65536, 'a'), false);
    check_filter(std::string("a\x00", 2), false);
    check_filter(std::string("\xC0\xAF", 2), false);
}

TEST(topic_match_table) {
    const MatchCase cases[] = {
        {"a/+/c", "a/b/c", true},
        {"a/+/c", "a/b/c/d", false},
        {"a/+/c", "a/b", false},
        {"a/+/c", "a//c", true},   // '+' 匹配空层级
        {"a/+", "a/b", true},
        {"a/+", "a/b/c", false},
        {"+", "a", true},
        {"+", "a/b", false},
        {"+/+", "a/b", true},
        {"#", "a", true},
        {"#", "a/b/c", true},
        {"a/#", "a/b", true},
        {"a/#", "a/b/c", true},
        {"a/#", "a/", true},
        {"a/#", "a", true},           // '#' 匹配父层：'a/#' 匹配 'a'（规范 4.7.1.2）
        {"sport/#", "sport", true},   // 规范 4.7.1.2 的例子
        {"sport/tennis/player1/#", "sport/tennis/player1", true},
        {"sport/tennis/player1/#", "sport/tennis/player1/ranking", true},
        {"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true},
        {"a/+/#", "a/b", true},       // '#' 可以匹配零层
        {"+/#", "a", true},
        {"a/b/#", "a", false},        // 前缀对不上，'#' 也救不了
        {"a/b", "a/b/c", false},
        {"a/b/c", "a/b", false},
        {"a/b", "a/c", false},
        {"sport/+/player", "sport/tennis/player", true},
        {"sport/+/player", "sport/tennis/x/player", false},
        {"/+", "/finance", true},
        {"+/+", "/finance", true},
        {"a//b", "a//b", true},
        {"a//b", "a/b", false},
    };

    for (const MatchCase& item : cases) {
        check_match(item.filter, item.topic, item.expected);
    }
}

TEST(topic_dollar_prefix_rule) {
    // 以 '$' 开头的主题不被首层通配符匹配
    check_match("#", "$SYS/x", false);
    check_match("+/x", "$SYS/x", false);
    check_match("+/+", "$SYS/x", false);
    check_match("$SYS/#", "$SYS/x", true);
    check_match("$SYS/+", "$SYS/x", true);
    check_match("$SYS/x", "$SYS/x", true);
    // 非首层的通配符不受影响
    check_match("a/#", "a/$b", true);
    check_match("a/+", "a/$b", true);
}

TEST(topic_match_rejects_invalid_input) {
    check_match("a/#/b", "a/b", false);   // 非法过滤器
    check_match("a/+", "a/#", false);      // 非法主题名
    check_match("", "a", false);
    check_match("a", "", false);
}
