#include "mqttc/topic.hpp"

#include "mqttc/wire.hpp"

namespace mqttc {
namespace {

constexpr size_t kMaxTopicLength = 65535;

bool first_level_is_wildcard(std::string_view filter) {
    const size_t end = filter.find('/');
    const std::string_view first =
        filter.substr(0, end == std::string_view::npos ? filter.size() : end);
    return first == "+" || first == "#";
}

} // namespace

bool valid_topic_name(std::string_view name) {
    if (name.empty() || name.size() > kMaxTopicLength) return false;
    if (!wire::valid_utf8(name)) return false;
    return name.find_first_of("+#") == std::string_view::npos;
}

bool valid_topic_filter(std::string_view filter) {
    if (filter.empty() || filter.size() > kMaxTopicLength) return false;
    if (!wire::valid_utf8(filter)) return false;

    size_t pos = 0;
    for (;;) {
        const size_t end = filter.find('/', pos);
        const bool last = end == std::string_view::npos;
        const std::string_view level = filter.substr(pos, last ? std::string_view::npos : end - pos);

        if (level.find('#') != std::string_view::npos && (level != "#" || !last)) return false;
        if (level.find('+') != std::string_view::npos && level != "+") return false;

        if (last) return true;
        pos = end + 1;
    }
}

bool topic_matches(std::string_view filter, std::string_view topic) {
    if (!valid_topic_filter(filter)) return false;
    if (!valid_topic_name(topic)) return false;
    if (topic[0] == '$' && first_level_is_wildcard(filter)) return false;

    size_t filter_pos = 0;
    size_t topic_pos = 0;

    for (;;) {
        const size_t filter_end = filter.find('/', filter_pos);
        const bool filter_last = filter_end == std::string_view::npos;
        const std::string_view filter_level =
            filter.substr(filter_pos, filter_last ? std::string_view::npos : filter_end - filter_pos);

        // '#' 代表父层与任意层子级（可以零层），所以到这里就直接命中：
        // "sport/#" 匹配 "sport"，"sport/tennis/#" 匹配 "sport/tennis"（规范 4.7.1.2）
        if (filter_level == "#") return true;

        const size_t topic_end = topic.find('/', topic_pos);
        const bool topic_last = topic_end == std::string_view::npos;

        // 主题已经没有这一层了，而过滤器还有：只有过滤器下一层正好是 '#' 才算匹配
        if (topic_last && !filter_last) return filter.substr(filter_end + 1) == "#";

        const std::string_view topic_level =
            topic.substr(topic_pos, topic_last ? std::string_view::npos : topic_end - topic_pos);

        if (filter_level != "+" && filter_level != topic_level) return false;
        if (filter_last) return topic_last;

        filter_pos = filter_end + 1;
        topic_pos = topic_end + 1;
    }
}

} // namespace mqttc
