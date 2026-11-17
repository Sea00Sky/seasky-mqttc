# 改动记录

格式照 [Keep a Changelog](https://keepachangelog.com/)；版本遵循语义化版本。
**每一节末尾都标清楚"验证到哪一步"**：没跑过的不写"应该没问题"。

## [0.2.0] — 未发布

工程化升级：把"能跑通的练习项目"变成"能被构建、集成、观测、验证的样子"。
协议支持范围不变（仍是 MQTT 3.1.1 / QoS 0、1）。

### 新增

- **构建与集成**：`project(mqttc VERSION 0.2.0)`；`find_package(Threads REQUIRED)`（修掉
  Linux 上 `std::thread` 的链接缺失）；安装导出 `mqttcTargets` + `mqttcConfig.cmake`
  （`find_dependency(Threads)`），下游可用 `find_package(mqttc CONFIG REQUIRED)` 并链
  `mqttc::mqttc`；`examples/consumer/` 是最小消费方示例。
  `-Wall -Wextra -Wpedantic -Werror` 改为只作用于本项目目标（`PRIVATE`），不再传给下游。
- **可观测性**：`include/mqttc/log.hpp` + `src/log.cpp`。注入式 `Logger`（默认没有 sink 就
  完全不打印、不碰 stdout/stderr、不碰全局状态）；结构化一行 `事件 key=value key=value`；
  `Stats` 扩到 16 项计数并配 `format_stats()` 稳定文本快照。CLI 加 `--log-level`、`--stats`。
- **资源上界**：`max_packet_bytes`（默认 16 MiB，超限断连并计 `oversize_packets`）、
  `max_inflight`（到顶 `publish()` 返回新增的 `Error::Busy`）。
- **重连增强**：退避抖动（`backoff_jitter_ratio`，随机种子可用 `rng_seed` 注入，抖动可复现）；
  失败分类（CONNACK 码 3 瞬时 → 退避；1/2/4/5 永久 → 新增 `State::Failed` 并停止重试，
  `failure_reason()` / `failure_detail()` 供调用方与 CLI 使用）；`max_connect_attempts`、
  `retry_on_permanent`。
- **多端点故障转移**：`include/mqttc/failover.hpp` + `src/failover.cpp`。`FailoverTransport`
  是 `Transport` 装饰器：一次 `connect()` 内按优先顺序试完所有端点，连上的成为下次首选，
  `switches()` 暴露切换次数。CLI 的 `-H` 支持 `host[:port],host[:port]` 并打印切换。
- **并发外壳**：`include/mqttc/async_client.hpp` + `src/async_client.cpp`。IO 线程 + 投递线程 +
  有界的命令队列与入站队列；`state()`/`stats()` 返回快照；`set_handler()` 可运行中更换；
  停机顺序"join IO → 排空队列 → join 投递"。约定见 [docs/THREADING.md](docs/THREADING.md)。
- **TLS（POSIX / OpenSSL 3）**：`include/mqttc/tls.hpp` + `src/tls_transport.cpp`，
  作为 `Transport` 的另一种实现（`Client` 零改动）。默认校验服务端证书（CA 文件/目录或系统
  信任库、SNI、DNS/IP 主机名校验）、支持 mTLS、握手与读写都有有界超时；证书校验失败映射为
  新增的 `Error::CertVerify`，`Client` 默认判永久失败。`Transport::last_error()` 透出
  OpenSSL 错误栈细节。CLI 加 `--tls/--cafile/--capath/--cert/--key/--sni/--insecure`
  （`--insecure` 显式关闭校验且会先警告）。用法与边界见 [docs/TLS.md](docs/TLS.md)。
  **Windows 侧尚未实现**：CMake 在 WIN32 + `MQTTC_ENABLE_TLS=ON` 时明确报错。
- **模糊测试**：`fuzz/fuzz_decode.cc`、`fuzz/fuzz_wire.cc`（libFuzzer，需要 clang）；
  以及不依赖 libFuzzer 的 `tests/test_fuzz_lite.cpp`（固定种子随机字节 + 合法报文变异）。
- **端到端**：mock broker 支持 QoS1 下行回 PUBACK、`--reject-code`、可选 TLS 与 mTLS；
  `e2e/make_certs.py` 在构建目录里运行时生成自签证书（不入库）。`run_e2e.py` 明文三个场景
  共 17 项检查（含多端点切换与永久失败退出码 3）；TLS 构建再加 12 项（单向收发、DNS/SNI、
  CA 不认、主机名不匹配、mTLS 正反例），共 29 项。CLI 加 `-H` 多端点、`--log-level`、
  `--stats`、TLS 选项，退出码分级（0 / 1 / 2 / 3）。

### 修复

- **packet id 泄漏**：`unsubscribe()` 分配的 id 在收到 UNSUBACK 时从不归还，每发一次 UNSUBSCRIBE
  就永久烧掉 65535 个 id 里的一个，泄漏到上限后发布/订阅恒返回 `WouldBlock`。现在
  `UNSUBACK` 会释放 id，掉线时也像 SUBACK 那样统一归还。
- **乱序/多余 ACK 会释放别人的 id**：`SUBACK` / `UNSUBACK` 以前无条件释放 id，一个乱序的
  SUBACK 可以把在途 PUBLISH 的 id 提前还回去，导致 id 复用、应答串线。现在先确认是自己等的
  id 才释放，否则计 `unexpected_packets` 丢掉。
- **"对端先发最后一包再关连接"会丢包**：读到 `Closed` 时立刻重连，把刚读到的字节留在缓冲里
  没解析。真实 broker 拒绝连接正是"发 CONNACK 错误码 + 关 socket"，于是永久失败被当成网络抖动，
  一路重试到超时。现在先解析缓冲、状态仍在网才判关闭。
- **接收缓冲每包一次 O(n) 搬运**：改为游标 + 必要时才压缩（已消费部分占到一半或超过 4 KiB）。
- **在等 ACK 的 id 集合上做线性扫描**：发得多、ACK 回得慢时退化成 O(n²)
  （实测 65535 次 UNSUBSCRIBE 后每轮 240 µs），改用 `unordered_set`。

### 变更

- `MQTTC_ENABLE_TLS` 默认 **ON**（锁定的范围）。POSIX 上由 OpenSSL 提供实现；
  Windows 上打开会在配置阶段明确报错（Windows 实现尚未编写/验证），需要显式
  `-DMQTTC_ENABLE_TLS=OFF`。TLS 打开后 `mqttcConfig.cmake` 会 `find_dependency(OpenSSL)`。
- 加错误码必须同一次编辑里补 `result.hpp` 的 `error_name`（那个 switch 没有 `default:`）。
  本次新增 `Busy`、`InvalidArgument`、`Tls`、`CertVerify`。
- README 删掉"零第三方依赖"与"不做 TLS / 多线程留给调用方"这类已经作废的说法，改为如实描述
  能力与验证范围。

### 验证到哪一步

- **真实跑过**（Windows 11 + w64devkit，GCC 16.2.0 / CMake 4.4.4）：
  默认配置构建零警告；`129 cases, 0 failed cases, 0 failed checks`；
  `ctest` 两项通过（`mqttc_tests` 与 `mqttc_e2e`，端到端 17/17）；
  `cmake --install` 后 `examples/consumer` 用 `find_package` 编译并运行。
- **真实跑过**（WSL2 Ubuntu 24.04.1，GCC 13.3.0 / Clang 18.1.3 / OpenSSL 3.0.13）：
  `src/transport_posix.cpp` 与 `src/tls_transport.cpp` 首次编译并运行；两套编译器零警告、
  `ctest` 各 2/2（e2e 含 TLS，29/29）；ASan + UBSan + LSan 全绿；TSan 全绿（129 个用例 +
  e2e，需 `sysctl vm.mmap_rnd_bits=28`）；覆盖率实测（含 TLS）行 90.9% / 函数 99.3% /
  分支 57.7%；`fuzz_decode`、`fuzz_wire` 各实跑 60 秒（1181 万 / 1824 万次执行）无崩溃。
- **没跑过，因此不作结论**：GitHub Actions（workflow 从未推送）、MSYS2、Windows 侧 TLS、
  性能基准、macOS、MSVC。

## [0.1.0] — 更早

从零实现 MQTT 3.1.1 客户端库与 CLI：`wire` / `topic` / `packet` 纯函数层、`Client` 单线程
状态机、`Transport` 抽象与 `InMemoryTransport` / `TcpTransport`、keepalive 与指数退避重连、
重连后重订阅、QoS1 重发、`mqttc sub` / `mqttc pub`、手写断言框架与 `e2e/mock_broker.py`。

验证：Windows/MinGW 下 58 个用例全绿 + 端到端跑通。
