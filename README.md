# mqttc

C++17 写的 MQTT 3.1.1 客户端库 + 命令行工具。报文编解码、主题匹配、QoS 0/1 重发、keepalive、
重连、多端点故障转移都是照 OASIS 规范自己实现的，**协议层没有任何第三方依赖**；异步外壳只用
`std::thread` 与标准库同步原语。没有依赖 paho 之类的现成客户端实现。

> 这是一个**按规范自己实现**的项目：目的是把协议细节和工程边界讲清楚，不是产品级替代品。
> TLS 已在 POSIX 上实现（OpenSSL），Windows 侧尚未实现（见[验证状态](#验证状态)）；
> 不做 QoS 2，不做 broker。

## 特性

| 项 | 状态 |
|---|---|
| 协议版本 | MQTT 3.1.1（protocol name `MQTT`，level `4`） |
| 报文 | CONNECT / CONNACK / PUBLISH / PUBACK / SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK / PINGREQ / PINGRESP / DISCONNECT |
| QoS | 0 与 1，QoS1 重发时置 `DUP` |
| 会话 | `clean session = 1` |
| 连接 | keepalive 心跳、等不到 PINGRESP 判死、指数退避重连、重连后自动重订阅 |
| 重连策略 | 退避抖动（可注入随机种子，抖动可复现）、瞬时/永久失败分类、尝试次数上限、多端点故障转移 |
| 主题 | 主题名与过滤器完整校验，`+` / `#` 通配符匹配，`$` 开头的主题不被首层通配符匹配 |
| 选项 | will 消息、用户名密码字段、接收缓冲与在途消息上限（到顶返回 `Busy`） |
| 可观测 | 注入式结构化日志（默认完全不打印）、16 项统计计数与稳定的文本快照 |
| 并发 | 单线程 `Client` 负责状态机；`AsyncClient` 外壳提供后台 IO 线程 + 投递线程 + 有界队列 |
| 集成 | `find_package(mqttc CONFIG)` + `mqttc::mqttc`，可 `install` 后给下游消费 |
| TLS | POSIX（OpenSSL 3）：CA 文件/目录、SNI、主机名校验、mTLS；证书错误明确上报并判永久失败 |
| 工具 | `mqttc sub` / `mqttc pub` 两个子命令，支持多端点、TLS、结构化日志、退出码分级 |
| 测试 | 129 个单元/行为用例 + 真 socket 端到端（自带的 Python mock broker，含 TLS 场景共 29 项检查） |

## 构建

需要 C++17 编译器与 CMake ≥ 3.20；TLS 默认开启，需要 OpenSSL 开发包（Debian/Ubuntu 是
`libssl-dev`；没有就显式 `-DMQTTC_ENABLE_TLS=OFF`）：

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure        # 或直接跑 build/mqttc_tests
```

Windows + MinGW 下加 `-G "MinGW Makefiles"`，并且目前必须加 `-DMQTTC_ENABLE_TLS=OFF`
（Windows 侧 TLS 尚未实现，默认开会在配置阶段明确报错）。编译选项是
`-Wall -Wextra -Wpedantic -Werror`（只作用于本项目自己的目标，不会传给下游），零警告。

主要选项：

| 选项 | 默认 | 说明 |
|---|---|---|
| `MQTTC_BUILD_CLI` | ON | 构建 `mqttc` 命令行工具 |
| `MQTTC_BUILD_TESTS` | ON | 构建单元测试与端到端测试 |
| `MQTTC_BUILD_ASYNC` | ON | 构建 `AsyncClient`（异步外壳） |
| `MQTTC_ENABLE_TLS` | ON | TLS（OpenSSL；POSIX）。Windows 上打开会明确报错（尚未实现） |
| `MQTTC_BUILD_FUZZERS` | OFF | libFuzzer 目标，需要 clang |
| `MQTTC_SANITIZE` | 空 | 例如 `-DMQTTC_SANITIZE=address,undefined`（GCC/clang） |
| `MQTTC_ENABLE_COVERAGE` | OFF | `--coverage` 构建 |

## 快速上手

需要一个 broker。仓库自带一个只够测试用的脚本化 broker（Python 3，标准库）：

```sh
python e2e/mock_broker.py --port 18831
```

再开两个终端：

```sh
./build/mqttc.exe sub -t 'demo/#' -H 127.0.0.1 -p 18831
./build/mqttc.exe pub -t demo/a -m hello -H 127.0.0.1 -p 18831 -q 1
```

`sub` 那一侧会打印：

```
已连接 127.0.0.1:18831，订阅 demo/#（QoS 0）
demo/a hello
```

把 broker 杀掉再起，客户端会自己退避重连、重连后重新订阅，第二条消息照样收到。
这套动作（加上多端点故障转移、永久失败退出码）已经写成脚本，一条命令跑完：

```sh
python e2e/run_e2e.py
```

### 多端点故障转移

`-H` 可以给一串 `host[:port]`，逗号分隔；连不上的会立刻跳到下一个，不等退避定时器。
连上的那个成为下次的首选（避免每次都先撞一遍已经死掉的端点）：

```sh
./build/mqttc.exe sub -t 'demo/#' -k 5 -H mqtt1.local:1883,mqtt2.local:1883 --log-level info
# 主用端点挂掉后： [mqttc] 已切换到备用端点 mqtt2.local:1883（第 1 次）
```

### TLS

默认开启（`MQTTC_ENABLE_TLS=ON`，需要 OpenSSL 开发包：Debian/Ubuntu 是 `libssl-dev`）。
实现是 `Transport` 的另一个实现 `TlsTransport`，`Client` 与状态机不知道它存在。

```sh
# 单向 TLS：只校验服务端
./build/mqttc.exe sub -t 'demo/#' -H broker.example -p 8883 --tls --cafile ca.crt

# mTLS：再带客户端证书与私钥
./build/mqttc.exe pub -t demo/a -m hello -H broker.example \
    --tls --cafile ca.crt --cert client.crt --key client.key
```

- `--tls` 且没给 `-p` 时默认端口是 8883；`-H` 仍可给多端点做故障转移。
- 证书校验默认开启：信任材料来自 `--cafile` / `--capath`（都不给则用系统默认信任库）；
  主机名校验用 `-H` 里的 host，`--sni` 可覆盖。
- `--insecure` 关闭服务端证书校验，CLI 会先打印警告。只用于实验——那样中间人可以冒充 broker。
- 证书校验失败是**永久失败**：`Error::CertVerify` → `Client::State::Failed`，CLI 退出码 3，
  不会当网络抖动无限重试。具体原因（OpenSSL 错误栈、verify 结果）经
  `Transport::last_error()` 透出，CLI 打到 stderr。

库内用法（`Client` 的写法完全不变）：

```cpp
mqttc::TlsOptions tls;
tls.ca_file = "ca.crt";
mqttc::TlsTransport transport("broker.example", 8883, tls);
mqttc::Client client(transport, options);
```

Windows 侧的 `TlsTransport` **尚未实现**（需要 MSYS2 + OpenSSL 环境来编写与验证）；
Windows 上 `MQTTC_ENABLE_TLS=ON` 会在 CMake 配置阶段直接报错，请用 `-DMQTTC_ENABLE_TLS=OFF`。
完整选项、错误语义与验证状态见 [docs/TLS.md](docs/TLS.md)。

### 日志与统计

库本身默认**不打印任何东西**（没有 sink 就是空转）。CLI 把结构化日志接到 stderr：

```sh
./build/mqttc.exe pub -t demo/a -m hello --log-level info --stats
# [mqttc:info] connected client_id=mqttc-cli session_present=false attempt=1 reconnects=0
# published=1 ...
```

`--log-level` 取 `error` / `warn` / `info` / `debug`；`--stats` 在退出时把 16 项计数按
一行一个 `key=value` 打到 stderr。stdout 只留给消息本身，管道里取 payload 不受影响。

退出码：`0` 正常、`1` 参数错误、`2` 连接超时（还会重试，但本次调用到期了）、
`3` 永久失败（凭据错、标识被拒之类，不再自动重试）。

命令行选项见 `./build/mqttc.exe -h`。

## 嵌入到自己的程序

安装后按包消费（`examples/consumer/` 就是这样一个最小例子，已验证能编译运行）：

```sh
cmake --install build --prefix /path/to/prefix
cmake -S examples/consumer -B build-consumer -DCMAKE_PREFIX_PATH=/path/to/prefix
```

```cmake
find_package(mqttc CONFIG REQUIRED)
target_link_libraries(your_app PRIVATE mqttc::mqttc)
```

```cpp
#include "mqttc/client.hpp"
#include "mqttc/transport.hpp"

mqttc::TcpTransport transport("127.0.0.1", 1883);
mqttc::Client client(transport, options, [](const mqttc::Message& message) {
    // 返回 false = 处理不过来，该条丢弃并计数
    return true;
});
client.start(mqttc::monotonic_ms());
// 单线程模型：由调用方按自己的节奏推进状态机
while (running) {
    client.pump(mqttc::monotonic_ms());
}
```

单线程模型要求调用方自己保证 `pump()` 的频率（不变量：`pump` 间隔 ≤ `min(keepalive, 重发间隔) / 4`）。
不想自己管线程就用异步外壳：它自己开一个 IO 线程按 tick 推进，用户回调在另一个线程上跑，
入站队列与命令队列都是有界的。

```cpp
#include "mqttc/async_client.hpp"

mqttc::AsyncClient client(std::make_unique<mqttc::TcpTransport>("127.0.0.1", 1883),
                          options, handler);
client.start();
client.wait_for_state(mqttc::State::Connected, 5000);
client.subscribe("demo/#");
client.publish("demo/a", payload);
// ...
client.stop();   // 先 join IO 线程，再让投递线程把队列排空后退出
```

约定写清楚（`docs/THREADING.md`）：`publish()` 返回 `Error::None` 只表示**已入队**，真正的连接状态
与在途上限由 IO 线程判断；**禁止在回调里调 `stop()`**（那是投递线程 join 自己，会死锁，
Debug 下会断言）。

## 测试

| 文件 | 测什么 |
|---|---|
| `tests/test_wire.cpp` | 大端读写、Remaining Length 变长整数（0 / 127 / 128 / 16383 / 16384 / 2097151 / 2097152 / 268435455）、严格 UTF-8 校验 |
| `tests/test_topic.cpp` | 主题名与过滤器校验、通配符匹配表、`$` 首层规则 |
| `tests/test_packet.cpp` | 11 类报文的手抄字节比对与往返、畸形输入（截断、超限、QoS 3、packet id 为 0、保留类型 0/15、多出字节…） |
| `tests/test_client.cpp` | 状态机：keepalive 发 PINGREQ、缺 PINGRESP 重连、重连后重订阅、QoS1 重发带 `DUP`、packet id 只在收到 PUBACK 后复用、逐字节喂数据 |
| `tests/test_client_extra.cpp` | 之前没覆盖的边界：CONNACK 超时边界、超长报文与接收缓冲上限、UNSUBACK 归还 id、乱序/多余的 ACK、"对端先发最后一包再关连接"的顺序 |
| `tests/test_reconnect.cpp` | 退避加倍与上限、抖动上下界与同种子可复现、瞬时/永久失败分类、尝试次数上限、keepalive 超时后重发 |
| `tests/test_failover.cpp` | 端点轮转顺序、切换计数、读写转发、`host[:port][,…]` 解析（含非法输入） |
| `tests/test_log.cpp` | 结构化日志的格式与转义、默认静默、级别过滤、统计快照覆盖每一项 |
| `tests/test_transport.cpp` | 真 `TcpTransport` 的失败路径：黑洞地址与不存在的域名在界内返回、未连接时读写返回 `Closed` |
| `tests/test_async.cpp` | 多线程发布、慢回调不阻塞 IO 线程（ticks 与 PINGREQ 时间戳）、队列满按条数丢弃且账目对得上、回调里再 publish、`stop()` 与 `publish()` 竞争、运行中换回调、停机排空队列 |
| `tests/test_fuzz_lite.cpp` | 固定种子的伪随机字节与合法报文变异，砸 `peek`/`decode`/`encode` 与主题校验：不崩、往返一致、结果稳定 |
| `fuzz/fuzz_decode.cc`、`fuzz/fuzz_wire.cc` | libFuzzer 目标（`-DMQTTC_BUILD_FUZZERS=ON`，需要 clang；已在 Linux 上实跑） |

客户端行为测试全部走内存传输层并显式传入时间戳，所以不需要真网络、不需要等真实秒数。
异步与多线程那部分必须用真线程，断言只锁行为与不变量，不锁精确耗时（连跑 5 次结果一致）。

## 验证状态

**Windows 11 + w64devkit（GCC 16.2.0 / CMake 4.4.4，Git-Bash）——真实跑过**：

- 默认配置构建零警告（`-Wall -Wextra -Wpedantic -Werror`），
  `129 cases, 0 failed cases, 0 failed checks`。
- `ctest` 2/2：`mqttc_tests` 与 `mqttc_e2e`；端到端 17/17，含多端点故障转移与永久失败退出码。
- `cmake --install` + `examples/consumer` 用 `find_package(mqttc CONFIG)` 编译并运行。

**WSL2 Ubuntu 24.04.1（GCC 13.3.0 / Clang 18.1.3，OpenSSL 3.0.13）——真实跑过**（2026-11-17）：

- 两套编译器**带 TLS** 全量构建（含首次编译 `src/transport_posix.cpp` 与 `src/tls_transport.cpp`）
  零警告，`ctest` 各 2/2。
- ASan + UBSan + LeakSanitizer（`-DMQTTC_SANITIZE=address,undefined`）：`129 cases, 0 failed`，
  `ctest` 2/2，无报告。
- **TSan（`-DMQTTC_SANITIZE=thread`）：`129 cases, 0 failed`，`ctest` 2/2**——这是并发外壳
  （`AsyncClient`）的门禁。前提是把 `vm.mmap_rnd_bits` 从 Ubuntu 默认的 32 降到 28：
  TSan 的影子内存与高熵 ASLR 冲突，否则连用例都起不来，直接
  `FATAL: ThreadSanitizer: unexpected memory mapping`、exit 66（环境问题，不是竞争报告）。
  本机的设置已写进 `/etc/wsl.conf` 的 boot 命令；CI 的 TSan job 里有同样的一步。
- 端到端（含 TLS）：**29/29 项通过**——明文收发、断线重连、多端点故障转移、CONNACK 4 永久失败，
  以及 TLS 单向收发、按 DNS 名（SNI + 主机名校验）、CA 不认 → 退出码 3、主机名不匹配 → 退出码 3、
  mTLS 不给证书连不上 / 给证书成功。
- 覆盖率（gcov + gcovr，`-DMQTTC_ENABLE_COVERAGE=ON`，**含 TLS**）：**行 90.9%（1442/1587）、
  函数 99.3%（143/144）、分支 57.7%（1402/2430）**。逐文件行覆盖：`wire`/`topic` 100%、
  `packet` 99%、`failover`/`client` 95%、`log` 96%、`async_client` 92%、`socket_posix` 91%、
  `transport_posix` 83%、`tls_transport` 61%（大量配置错误/超时分支没有用例走到，这个数字不修饰）。
- libFuzzer（clang 18 + `-fsanitize=fuzzer,address,undefined`，`MQTTC_BUILD_FUZZERS=ON`）：
  `fuzz_decode` 1181 万次执行、`fuzz_wire` 1824 万次执行，**无崩溃、无泄漏报告**。
  首轮运行撞出的两处问题都在**fuzzer 目标自己的不变量**里（"再编码字节数必须与原文相等"），
  不在库里——3.1.1 允许 Remaining Length 非最短编码（`0x80 0x00` 表示 0），编码器则输出
  最短形式。不变量已改成语义往返 + 规范化幂等，并补了两条单元回归用例。
  这是**各 60 秒的随机搜索**，不是"证明没有 bug"。

**哪些还没跑过**（不声称结论）：

- GitHub Actions：`.github/workflows/ci.yml` 是写好的配置，**从未推送、从未执行**。
- MSYS2/ucrt64：本机没装；Windows 侧 TLS 的实现与验证都等着这个环境。目前 Windows 构建
  显式 `-DMQTTC_ENABLE_TLS=OFF`（w64devkit），Windows 上默认配置会因 TLS 未实现而明确报错。
- macOS：POSIX 传输层理论上走同一份代码（含 `SO_NOSIGPIPE` 分支），但**没有环境，未验证**。
- MSVC：未验证，也不做支持承诺。
- 性能：没有做过基准，也没有跟别的客户端对拍过。

## 限制清单

明确**不做**的东西：

| 不做 | 原因 |
|---|---|
| MQTT 5.0 | 体量翻倍，3.1.1 覆盖设备侧绝大多数场景 |
| QoS 2 | 四段握手（PUBREC/PUBREL/PUBCOMP）对本项目的目标没有额外价值；收到 QoS2 直接判协议错误 |
| 会话持久化 / 离线消息队列 | 只做 clean session |
| WebSocket 传输 | 只做裸 TCP |
| broker 实现 | `e2e/mock_broker.py` 只够测试用，不是可用服务端 |
| 保留消息语义、`$SYS` 主题处理、共享订阅 | 未实现 |
| 异步 IO 模型（epoll / IOCP） | 继续阻塞 socket + `poll`/`select` + 超时；IO 线程只有一个 |

其他边界：

- **TLS 只在 POSIX 上**：Linux 侧已实测（单向、mTLS、CA/主机名反例）；Windows 侧尚未实现。
  在 Windows 或没有 OpenSSL 的环境里请显式 `-DMQTTC_ENABLE_TLS=OFF`，此时连接是明文。
- **Windows TLS 未验证**：没有 `TlsTransport` 的 Windows 实现，也没有 MSYS2 环境跑过。
- **慢消费者会丢消息**：回调返回 `false` 表示处理不过来，该条被丢弃并计数，QoS1 也不会重投
  （仍然回 PUBACK，避免 broker 无限重发）。异步外壳的入站队列满了同样丢并计数。
  这是"实时性优先于完整性"的取舍，**不是可靠性队列**。
- **核心状态机仍是单线程的**：并发能力来自 `AsyncClient` 外壳，不是 `Client` 本身。
  直接多线程用同一个 `Client` 是不支持的。
- **不是生产级实现**，没有跟其它客户端做过功能或性能对拍。

## 目录结构

```
include/mqttc/
  result.hpp        最小 Result<T> 与错误码
  wire.hpp          大端读写、Remaining Length、UTF-8 校验
  topic.hpp         主题校验与通配符匹配
  packet.hpp        11 类报文的 encode/decode
  log.hpp           注入式日志与结构化一行
  transport.hpp     Transport 抽象 + InMemoryTransport + TcpTransport
  tls.hpp           TLS 传输层（OpenSSL；POSIX）
  failover.hpp      多端点故障转移装饰器与端点串解析
  types.hpp         QoS / Message / ClientOptions / State / Stats
  client.hpp        单线程状态机
  async_client.hpp  异步外壳（IO 线程 + 投递线程 + 有界队列）
src/                wire / topic / packet / client / log / failover
                    / async_client / tls_transport
                    / transport_win / transport_posix / socket_posix.hpp
tools/              mqttc_main.cpp（CLI）
tests/              手写断言框架 + 11 个测试文件 + support.hpp（测试用假传输）
e2e/                mock_broker.py（脚本化 broker，可选 TLS）、make_certs.py、
                    run_e2e.py（端到端脚本）
fuzz/               libFuzzer 目标
examples/consumer/  用 find_package 消费本库的最小例子
docs/               DESIGN.md（取舍）、THREADING.md（并发约定）、TLS.md（TLS 用法与边界）
```

实现细节与取舍记录在 [docs/DESIGN.md](docs/DESIGN.md)，并发约定在
[docs/THREADING.md](docs/THREADING.md)，TLS 在 [docs/TLS.md](docs/TLS.md)，
改动记录在 [CHANGELOG.md](CHANGELOG.md)。

## 许可

[MIT](LICENSE)。拿去用、改、嵌进别的项目都可以，保留版权声明即可。

这是按规范自己写出来的项目，**不提供任何担保**。
