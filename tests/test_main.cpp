#include <cstdio>

#include "test.hpp"

int main() {
    // 不缓冲：重定向到管道时也能一行一行看到进度（CI 日志不用等退出才刷出来）。
    // Windows 上 _IOLBF 对管道会退化成全缓冲，所以这里用 _IONBF。
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    const int total = static_cast<int>(t::cases().size());
    int failed_cases = 0;

    for (const t::Case& test_case : t::cases()) {
        const int before = t::failure_count();
        std::printf("[ RUN  ] %s\n", test_case.name);
        test_case.fn();
        const int failures = t::failure_count() - before;

        if (failures == 0) {
            std::printf("[  OK  ] %s\n", test_case.name);
        } else {
            ++failed_cases;
            std::printf("[ FAIL ] %s (%d failed checks)\n", test_case.name, failures);
        }
    }

    std::printf("\n%d cases, %d failed cases, %d failed checks\n", total, failed_cases,
                t::failure_count());
    return failed_cases == 0 ? 0 : 1;
}
