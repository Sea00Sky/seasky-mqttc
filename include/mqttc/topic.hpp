#pragma once

// 主题名校验、主题过滤器校验与通配符匹配（规范 4.7）。

#include <string_view>

namespace mqttc {

// 主题名：非空、合法 UTF-8、不含 '+' 与 '#'、长度不超过 65535
bool valid_topic_name(std::string_view name);

// 主题过滤器：非空、合法 UTF-8；'#' 独占一层且只能是最后一层；'+' 必须独占一层
bool valid_topic_filter(std::string_view filter);

// 匹配规则含 $ 首层例外：首层为通配符的过滤器不匹配以 '$' 开头的主题名
bool topic_matches(std::string_view filter, std::string_view topic);

} // namespace mqttc
