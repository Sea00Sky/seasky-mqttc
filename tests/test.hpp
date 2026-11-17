#pragma once

// 手写测试框架：注册表 + 几个断言宏，不引第三方。

#include <cstdio>
#include <string>
#include <vector>

#include "mqttc/result.hpp"

namespace t {

struct Case {
    const char* name;
    void (*fn)();
};

inline std::vector<Case>& cases() {
    static std::vector<Case> registry;
    return registry;
}

inline int& failure_count() {
    static int count = 0;
    return count;
}

inline void report(const char* file, int line, const std::string& text) {
    ++failure_count();
    std::printf("    %s:%d: %s\n", file, line, text.c_str());
}

struct Registrar {
    Registrar(const char* name, void (*fn)()) { cases().push_back(Case{name, fn}); }
};

template <typename T>
inline void expect_error(const mqttc::Result<T>& result, mqttc::Error expected, const char* what,
                         const char* file, int line) {
    if (result.ok()) {
        report(file, line, std::string(what) + ": expected failure, got success");
        return;
    }
    if (result.error() != expected) {
        report(file, line, std::string(what) + ": expected " + mqttc::error_name(expected) +
                                ", got " + mqttc::error_name(result.error()));
    }
}

} // namespace t

#define TEST(name)                                               \
    static void test_##name();                                   \
    static ::t::Registrar registrar_##name(#name, &test_##name); \
    static void test_##name()

#define CHECK(cond)                                                     \
    do {                                                                \
        if (!(cond)) ::t::report(__FILE__, __LINE__, "CHECK(" #cond ")"); \
    } while (0)

#define CHECK_EQ(actual, expected)                                                \
    do {                                                                          \
        if (!((actual) == (expected))) {                                          \
            ::t::report(__FILE__, __LINE__, "CHECK_EQ(" #actual " == " #expected ")"); \
        }                                                                         \
    } while (0)

#define REQUIRE(cond)                                                       \
    do {                                                                    \
        if (!(cond)) {                                                      \
            ::t::report(__FILE__, __LINE__, "REQUIRE(" #cond ")");           \
            return;                                                         \
        }                                                                   \
    } while (0)

#define EXPECT_ERROR(result, error_code) \
    ::t::expect_error((result), (error_code), #result, __FILE__, __LINE__)
