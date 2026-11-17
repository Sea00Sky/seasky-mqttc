# 贡献指南

这是个人项目，节奏随意；但只要动代码，下面这几条就是硬约束。

## 环境与构建

需要 C++17 编译器与 CMake ≥ 3.20，以及 OpenSSL 开发包（TLS 默认开启）：

```sh
# Debian / Ubuntu
sudo apt install -y libssl-dev
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

没有 OpenSSL 或者用 Windows 时显式关掉 TLS（`-DMQTTC_ENABLE_TLS=OFF`）：Windows 侧的
`TlsTransport` 还没实现，打开会在配置阶段报错说明原因。

Windows 上本项目的实测环境是 w64devkit（GCC 16.2.0、CMake 4.4.4），生成器用：

```sh
cmake -S . -B build -G "MinGW Makefiles" -DMQTTC_ENABLE_TLS=OFF
cmake --build build
ctest --test-dir build --output-on-failure
```

`.github/workflows/ci.yml` 里还写了 MSYS2 ucrt64（`-G Ninja`）与 Linux（ubuntu-24.04，GCC / Clang，
含 sanitizer 变体）的构建命令。Linux 那一组已在 WSL2 Ubuntu 24.04.1 上按同样选项实跑过；
**MSYS2 与 GitHub Actions 本身尚未执行**——workflow 从未推送。

相关 CMake 选项（都有默认值，不设也能构建）：

| 选项 | 默认 | 说明 |
|---|---|---|
| `MQTTC_BUILD_CLI` | ON | 命令行工具 |
| `MQTTC_BUILD_TESTS` | ON | 单元 / 行为测试 |
| `MQTTC_BUILD_ASYNC` | ON | 异步外壳（IO 线程 + 有界队列） |
| `MQTTC_ENABLE_TLS` | ON | TLS（POSIX/OpenSSL）；Windows 上打开会报错（未实现） |
| `MQTTC_BUILD_FUZZERS` | OFF | libFuzzer 目标，需要 clang |
| `MQTTC_SANITIZE` | 空 | `address,undefined` 或 `thread` |
| `MQTTC_ENABLE_COVERAGE` | OFF | gcov 覆盖率 |

`MQTTC_BUILD_FUZZERS` 需要 clang（libFuzzer 由 clang 提供）；`MQTTC_SANITIZE` 则由编译器支持
决定，GCC 与 clang 都认 `-fsanitize=`。**Windows/MinGW 上这两项都跑不了**（没有 clang、
没有 sanitizer 运行时）；Linux 侧已在 WSL2 Ubuntu 24.04.1 上实跑过 ASan/UBSan/TSan 与两个
fuzzer 目标（见 README"验证状态"）。别把"应该能过"当成结论写进提交信息。

## 改动流程

1. 动手前先跑一遍 `ctest --test-dir build --output-on-failure`，确认基线是绿的。
2. 新增或改变行为，就要带用例。协议层加在 `tests/test_packet.cpp` / `tests/test_wire.cpp` /
   `tests/test_topic.cpp`，状态机加在 `tests/test_client*.cpp` / `tests/test_reconnect.cpp`，
   异步外壳加在 `tests/test_async.cpp`。TLS 的握手/证书行为由端到端覆盖
   （`e2e/`，证书现场生成），不放进不碰 socket 的单元用例里。
3. 本项目目标的编译选项是 `-Wall -Wextra -Wpedantic -Werror`（见 CMakeLists.txt 里的
   `mqttc_target_defaults`）。新代码必须零告警：报警就是错误。
4. 提交前 `ctest` 全绿。

## 代码约定

从现有代码里能看出来的几条，照着写就不会突兀：

- **错误一律用 `Result<T>` 与 `Error` 返回，不用异常。** 编解码失败、IO 失败都走返回值；
  `WouldBlock` 是"数据还没到"的正常状态，不是失败。资源用 RAII 管（socket、缓冲区）。
- **分层**：`wire` / `topic` / `packet` 是纯函数（输入字节或结构体，输出结构体或 `Result`，
  不碰 socket、不碰时间、不碰全局状态）；`Client` 是状态机，只依赖 `Transport` 抽象；
  `TcpTransport` / `FailoverTransport` / `InMemoryTransport` 是传输层实现。加功能前先想清楚
  它属于哪一层，别让纯函数层去碰 socket。
- **时间靠注入，不靠时钟**：`Client::pump(now_ms)` 的所有定时判断都用调用方给的毫秒数；
  `monotonic_ms()` 只在 CLI 与异步外壳里用。keepalive、重发、退避因此都能用确定的数字测出来。
- **测试框架**在 `tests/test.hpp`（手写，无第三方）：`TEST(...)` 注册用例，断言用 `CHECK` /
  `CHECK_EQ` / `REQUIRE` / `EXPECT_ERROR`。拼服务端报文、把客户端推进到某个状态的小工具在
  `tests/support.hpp`。
- **测试怎么做到不 sleep**：核心状态机与协议层的用例全部走 `InMemoryTransport` 并显式传入
  时间戳，所以不碰真网络、不等真实秒数。异步外壳（`AsyncClient`）的用例会用真实线程与毫秒级
  等待（见 `tests/test_async.cpp`），但传输层仍是内存实现；只有端到端测试（`e2e/`，自带
  Python mock broker）才真正起 socket。
- **注释写"为什么"，不复述代码**：解释取舍、规范依据、踩过的坑，而不是把 `i++` 翻译成中文。

### 加错误码的一个坑

`include/mqttc/result.hpp` 里的 `error_name(Error)` 是一个**没有 `default:` 的 switch**。新增
枚举值时，必须在同一次编辑里补上对应的 `case`——否则 `-Wswitch` 配上 `-Werror` 会让编译直接
失败。这是故意留的：漏掉一处，编译器会当场告诉你。

## 提交信息

一句话说清"做了什么"和"为什么"，别贴没有实际验证过的结论（比如"应该能在 Linux 上跑"）。
只改文档也一样。

## 报告问题

开 issue 时请附上：

- 平台与编译器版本（例如 Windows 11 + w64devkit GCC 16.2.0、CMake 4.4.4）；
- `cmake` 的完整配置行（含用到的 `-D` 选项）；
- `ctest` 或可执行文件的**原始输出**，不要只贴结论。
