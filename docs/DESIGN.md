# 设计说明

记录"为什么这么写"。规范细节以 OASIS MQTT 3.1.1 为准；这里只写取舍。

## 1. 分层：纯函数层、状态机、IO 层分开

```
wire / topic / packet      纯函数：输入字节或结构体，输出结构体或 Result
        ↑
     client                状态机：只依赖 Transport 抽象，单线程
        ↑
    transport              Transport 抽象 → InMemoryTransport（测试）/ TcpTransport（真连）
                           failover 是它上面的一层装饰器
        ↑
   async_client            所有权外壳：IO 线程 + 投递线程 + 两个有界队列（唯一的多线程部分）
```

`wire` / `topic` / `packet` 不碰 socket、不碰时间、不碰全局状态，所以协议层可以穷举测试：
不用起 broker、不用等网络、不用管偶发失败。`Client` 只依赖 `Transport` 接口，测试里换成内存实现
就能确定性地验证状态机。

时间也是注入的：`Client::pump(now_ms)` 的所有定时判断都用调用方给的毫秒数，
`monotonic_ms()` 只在 CLI 与异步外壳里用。keepalive、重发、退避因此都能用确定的数字测出来。

## 2. 测试策略

- 协议层：手抄规范里的字节序列做参照，往返（encode → decode）与逐项畸形输入都覆盖。
- 状态机：`InMemoryTransport` 脚本化喂字节，包括**一次只喂一个字节**，用来证明实现不假设
  "一次 read 就能拿到一个完整报文"。分帧靠 `peek_packet_size()` 先量长度再等够字节。
- 假传输要**像真 socket**：`InMemoryTransport::read` 在"对端已经关了但缓冲里还有字节"时先交付字节、
  下一次才报 `Closed`。这条顺序不模仿，就测不出"对端先发最后一包再关连接"这类丢包。
- 端到端：`e2e/mock_broker.py` 在真 socket 上起一个只够测试的 broker，验证真的能收发、
  杀掉 broker 后自愈、多端点故障转移、CONNACK 永久失败以退出码 3 结束
  （`e2e/run_e2e.py` 一条命令跑完；明文 17 项，TLS 构建再加 12 项）。
- 不依赖 libFuzzer 的一层下限：`tests/test_fuzz_lite.cpp` 用固定种子的伪随机字节与合法报文变异
  砸纯函数，断言"不崩 + 往返一致 + 结果稳定"。它跑在每一个编译器上；真正的 fuzzer 目标在
  `fuzz/` 下，只有 clang 才构建。
- 必须用真线程的部分（`tests/test_async.cpp`）：断言只锁**行为与不变量**，不锁精确耗时。
  例如"慢回调没有拖住 IO 线程"用"这段时间里 ticks 至少涨了 20"（回调跑在 IO 线程上的实现只会涨
  个位数），而不是断言某个毫秒数。

## 3. 取舍

### 3.1 慢消费者直接丢

`pump()` 把消息交给回调，回调返回 `false` 表示处理不过来：该条丢弃，计入 `stats.dropped`。
异步外壳把这条语义直接复用：转发器只负责入队，队列满就返回 `false`，核心照旧计数。

- 为什么不阻塞？设备侧要的是实时性，卡在一条老消息上比丢一条更糟。
- 为什么 QoS1 丢了还回 PUBACK？因为丢是**应用层**的选择，协议层已经完整收到并校验了；
  不回 ACK 会让 broker 一遍遍重发同一条，把实时性彻底毁掉。
- 代价必须说清楚：**这不是可靠性队列**，没有重投、没有落盘、没有背压。要可靠就得自己在回调里
  做缓冲，或者换 QoS2（本项目不做）。

### 3.2 不用异常，用 `Result<T>` + 错误码

- 构造/编解码失败、IO 失败都返回 `Result<T>`（约 20 行，`std::optional<T>` + `Error`）。
- 错误码只有一套：`None` / `WouldBlock` / `Closed` / `Timeout` / `Oversize` / `ProtocolError` /
  `Busy` / `InvalidArgument`。`WouldBlock` 是"数据还没到"的正常状态，不是失败——分帧和重连都靠它
  区分"要等"和"坏了"。
- `Busy` 专指"到了自己的资源上限"（在途消息数、命令队列长度），调用方该退让而不是重试到死。
  `InvalidArgument` 专指调用方传进来的参数不成立（空端点表、解析不了的 `host:port`）。
- 返回 `Error` 的接口（`Transport::connect/write`、`Client::publish/subscribe/unsubscribe`）用
  `Error::None` 表示成功，避免为一个空结果再造一个类型。
- 加错误码必须同一次编辑里补 `error_name`：那个 switch 没有 `default:`，漏一个就是
  `-Wswitch -Werror` 编译失败——把"忘了同步"变成编译期错误，是故意的。
- 资源一律 RAII：socket、Winsock 初始化（函数内 static 守卫）、缓冲区。

### 3.3 不追求零拷贝，但也不做无谓的搬运

payload 用 `std::vector<uint8_t>`，发布时 `move` 进去。

- MQTT 报文要把固定头、Remaining Length、可变头、payload 拼在一起，零拷贝要么让接口长出一堆
  `iovec`，要么让调用方自己维护缓冲区，复杂度立刻上一个台阶。
- 收报文用**游标**而不是每包 `erase`：`incoming_` + `incoming_pos_`，只有"已消费的部分"占到
  一半或超过 4 KiB 时才搬一次剩下的字节。逐字节喂数据的用例保证这个改动没改行为。
- 结论：**可读性优先**，但不在热路径上做 O(n) 的搬运。

### 3.4 日志：注入式、不碰全局、默认静默

- 库里没有全局 logger，也没有日志宏：`ClientOptions::logger` 是一个装着
  `std::function<void(LogLevel, const std::string&)>` 的值类型。没人装 sink 时 `enabled()` 为假，
  连字符串都不拼，不碰 stdout/stderr。
- 只记"状态变化与异常"（连上、被拒、重连、协议错误、丢弃、永久失败），热路径一个点都不打。
  需要看细节时把级别开到 `debug`。
- 结构化一行的格式是稳定的 `事件 key=value key=value`，值里有空格/引号/等号才加引号
  （`LogLine::field` 负责转义）。字符串必须走 `const char*` / `string_view` 重载：少了那条，
  字面量会隐式转成 `bool`，日志里就会出现 `reason=true` 这种鬼东西。
- 统计是稳定的文本快照（`format_stats()`，一行一个 `key=value`），不引 JSON 库、不引指标库。

### 3.5 资源上界写进选项，不在库里"看情况"

| 选项 | 默认 | 防的是什么 |
|---|---|---|
| `max_packet_bytes` | 16 MiB | 协议允许 256 MiB，但那是**对端说了算**的数字：不设上限，一个乱报长度的对端就能让本端缓冲 256 MiB |
| `max_inflight` | 1024 | 在途 QoS1 消息数；到顶 `publish()` 返回 `Busy`，库不替调用方无限排队 |
| `inbound_queue_capacity` / `inbound_max_bytes` | 256 条 / 4 MiB | 异步外壳的入站队列：条数与字节数同时限（一条超大消息也不能顶穿内存） |
| `command_queue_capacity` | 1024 | 异步外壳的命令队列；满了 `publish()` 立刻返回 `Busy` |
| `max_connect_attempts` | 0（不限） | 连不上就一直试？给个上限，到顶判永久失败，调用方能拿到明确信号 |

## 4. 状态机与定时

状态：`Idle`（未发起）→ `WaitingConnAck` → `Connected`；掉线后进 `Backoff` 等下一次重连；
判定为**永久失败**时进 `Failed` 并停止自动重试（`start()` 可以再来一轮，计数与退避会清零）。

- **keepalive**：空闲满 `keepalive` 秒就发 PINGREQ；发出后再过一个 `keepalive` 秒仍没等到
  PINGRESP，判链路已死 → 退避重连（计入 `stats.keepalive_timeouts`）。规范要求客户端在 1.5 倍
  时间内发出报文，这里取"满 1 倍就发"，比要求更积极。
  任何报文的发送都会刷新"最后活动时间"，所以频繁收发的连接不会额外发心跳。
- **CONNACK 超时**：10 秒没等到 CONNACK 就按连接失败处理，避免卡在 `WaitingConnAck`。
- **退避**：`min(上次退避 × 2, reconnect_max_ms)`，第一次等于 `reconnect_min_ms`；
  收到成功 CONNACK 后清零，所以下一次掉线又从下限开始，不会一路涨到上限。
- **抖动**：实际等待是 `base × (1 ± backoff_jitter_ratio)`（默认 ±20%）。没有抖动的话，
  一批同时掉线的设备会在同一毫秒一起回来。随机源是内置的 xorshift64*，种子可通过
  `ClientOptions::rng_seed` 注入（0 = 用单调时钟自动播种），所以抖动**可单测可复现**，
  不会变成偶发失败。抖动比例设为 0 就退回精确的 `base`，测试里断言精确值时用这个。
- **失败分类**：CONNACK 返回码 3（服务不可用）当瞬时失败，继续退避重试；
  1（协议版本不支持）/ 2（标识符被拒）/ 4（凭据错）/ 5（未授权）当**永久失败**，
  进 `Failed` 并把原因标签与配套数字暴露给调用方（`failure_reason()` / `failure_detail()`），
  CLI 据此翻译成人话并以退出码 3 结束。`retry_on_permanent` 可以显式覆盖这个判断。
  判据是"再试一次结果会不会不同"，不是"错误严重不严重"。
- **packet id 生命周期**：位图 + 游标轮转，`1..65535`，`0` 不合法；只在收到 PUBACK（或
  SUBACK/UNSUBACK）后才释放，所以在途消息的 id 不会被复用。掉线时会把"没等到 SUBACK/UNSUBACK
  的 id"一并归还，否则重连几轮就会把 id 用光。
  等 ACK 的那一组 id 用 `unordered_set` 而不是顺序扫：顺序扫在"发得多、ACK 回得慢"时会退化成 O(n²)
  （这个坑实测复现过，65535 次 UNSUBSCRIBE 后每轮 240 µs）。
- **多余的 ACK 不许释放别人的 id**：`SUBACK` / `UNSUBACK` 先确认 id 确实是自己在等的，
  不是就计入 `stats.unexpected_packets` 原样丢掉。曾经无条件释放，结果是"在途 PUBLISH 的 id
  被一个乱序 SUBACK 提前还回去"→ id 复用 → 两条报文的应答串在一起。
- **重连后做什么**：先重发所有已登记的订阅（一条 SUBSCRIBE 装多个过滤器），再把所有未确认的
  QoS1 消息带 `DUP=1` 重发，重发间隔计时同时重置。
- **"对端先发最后一包再关连接"**：一读到 `Closed` 不能立刻重连——刚读到的字节还在缓冲里没解析。
  正确顺序是"先解析完缓冲，若状态仍在网再判关闭"。真实 broker 拒绝连接正是
  "发 CONNACK 错误码 + 关 socket"，顺序错了就会把永久失败当成网络抖动一路重试到超时。

## 5. 与规范相关的几个判断

- Remaining Length 每字节低 7 位是数据、最高位表示"还有后续"，最多 4 字节，上限 268435455；
  第 5 个字节出现即判协议错误。
- `SUBSCRIBE` / `UNSUBSCRIBE` 的固定头标志位必须是 `0b0010`，其它报文标志位必须为 0，
  否则判协议错误。
- QoS 0 的 PUBLISH 不得置 `DUP`；QoS > 0 必须带非零 packet id。
- 主题过滤器里 `#` 必须独占一层且只能是最后一层，`+` 必须独占一层；主题名里不允许出现通配符。
  `$` 开头的主题不被首层的 `+` / `#` 匹配。
- **`#` 匹配父层**：`sport/#` 匹配 `sport`，`sport/tennis/player1/#` 匹配 `sport/tennis/player1`
  （规范 4.7.1.2：the multi-level wildcard represents the parent and any number of child levels）。
  `+` 没有这一条：`a/+` 不匹配 `a`。曾经按"层级数必须一致"实现过，是错的，测试里补了规范原文
  的那几个例子锁住行为。
- **QoS 2 一律拒绝**：包括 CONNECT 的 will QoS、SUBSCRIBE 请求的 QoS、SUBACK 返回的 `0x02`。
  这些值在规范里合法，但本库不做四段握手。宁可在收到时直接报协议错误，也不静默走进一个没有
  实现的状态——这是**故意的不兼容**，不是遗漏。

## 6. 并发模型（`AsyncClient`）

核心 `Client` **保持单线程不变**，并发能力由一层所有权外壳提供。这是刻意的：状态机里所有
"下一步该干什么"的判断都是线性的，把锁塞进去会让每一条路径都要重新论证一边。

- 只有 IO 线程碰 `Client` 与 `Transport`；应用线程只往命令队列里放东西。
  这条规则是"数据竞争能讲清楚"的全部原因——不是"锁加得多"。
- 用户回调只跑在投递线程上，且调用时**不持任何库内锁**（把 handler 的 `shared_ptr` 拷出来再解锁调用），
  所以回调里可以再 `publish()`，也可以慢慢处理。
- 快照：`state()` / `stats()` 返回 IO 线程每 tick 在锁下写的一份拷贝，最多滞后一个 tick。
  好处是不用把内部成员暴露给别的线程，代价是"刚发起的动作立刻查状态可能还是旧的"。
- `publish()` 的返回值语义：`None` = 已入队（不保证已发出）、`Closed` = 当前快照不是
  `Connected` 或已停机、`Busy` = 命令队列满、`ProtocolError` = 主题/过滤器非法（这类在调用线程
  就判掉，不占队列）。这样应用线程永远不会阻塞在库上。
- 停机顺序：置停止位 → 唤醒两个条件变量 → `join(IO)` → 宣布"队列不会再增长" →
  通知投递线程 → `join(投递)`。IO 先 join，所以最后一次 pump 入队的消息仍会被投递；
  投递线程把队列排空后退出，已收到的消息不会因为停机被丢。
- `stop()` 是幂等的，但**不能从回调里调**，那是投递线程 join 自己（Debug 下有断言）。
- 传输层的前提：`read()` 必须有界（自带实现都基于 `poll`/`select` 超时返回 `WouldBlock`）。
  `stop()` 不会从别的线程去关传输层——那会破坏"只有 IO 线程碰 transport"这条规则；
  换个无限阻塞在 `read()` 上的传输层，`stop()` 就得一直等它。
- tick 的不变量：`tick_ms ≤ min(keepalive_ms / 4, publish_retry_ms / 4)`。tick 太粗，定时器精度
  就会吃掉 keepalive 的裕量。
- 锁的顺序（避免死锁的唯一约定）：`mutex_`（命令队列/停止位/快照）、`inbox_mutex_`（入站队列）、
  `handler_mutex_`（取换回调）三者互不嵌套，只有"持 `inbox_mutex_` 时取 `handler_mutex_`"这一处
  单向顺序。

## 7. 多端点故障转移（`FailoverTransport`）

做成 `Transport` 装饰器，而不是改 `Client`：核心零改动，也让"从一堆端点里挑一个"这件事
能脱离 MQTT 单独讲清楚。

- `Client::begin_connect()` 每次都是"`close()` 再 `connect()`"。装饰器把一次 `connect()` 变成
  "按优先顺序依次试所有端点"：撞上死掉的端点立刻换下一个，**不用等退避定时器**。
- 连上的那个成为首选（`preferred_endpoint()`），下次先试它，避免每次都先撞一遍已经死掉的端点。
  稳定优先：主端点复活了也不抢回来。
- `switches()` 只统计"撞了端点后换到别的端点并连上"的次数，不污染 `Client::Stats`。
- 代价：一次 `connect()` 最坏耗时是"端点个数 × 单端点预算"（每个端点的超时由工厂造出来的
  传输层自己管）。端点表长的时候要把单端点预算调小——CLI 就把它收到了 5 秒。

## 8. TLS（`TlsTransport`）

TLS 同样是 `Transport` 的另一个实现，不是塞进 `TcpTransport` 的一个开关。这样 `Client`、
`AsyncClient`、`FailoverTransport` 全部零改动；测试里也仍然可以用内存传输层跑状态机。
当前只有 POSIX 实现（Linux/macOS 的 OpenSSL 路径）；Windows 侧尚未实现。

### 8.1 连接与校验

- **socket 全程非阻塞**：TLS 的读写靠 `SSL_ERROR_WANT_READ/WANT_WRITE` 驱动，由本层 `poll`
  等就绪。TCP 连接复用 `socket_posix.hpp` 的 `connect_tcp()`（与 `TcpTransport` 同一份，
  含连接超时与逐地址尝试）。
- **证书校验默认开启**（`verify_peer=true`）：`SSL_CTX_set_verify(SSL_VERIFY_PEER)`；
  信任材料来自 `ca_file`/`ca_path`，都为空则 `SSL_CTX_set_default_verify_paths()`（系统信任库）。
- **主机名**：非 IP 字面量 → `SSL_set_tlsext_host_name`（SNI）+ `SSL_set1_host`（校验证书里
  的 DNS SAN）；IP 字面量 → `X509_VERIFY_PARAM_set1_ip_asc`（证书里要有 IP SAN）。
- **mTLS**：`cert_file` + `key_file`，加载后 `SSL_CTX_check_private_key()` 确认配对；
  只给其中一个算配置错误。
- **握手有 deadline**（`handshake_timeout_ms`，默认 10 秒），到期返回 `Error::Timeout`。
- `SSL_CTX` 跨重连复用，`SSL` 每次 `connect()` 重建。

### 8.2 读写契约

- `read()` 永远不阻塞：`WANT_READ/WANT_WRITE` → `WouldBlock`；`ZERO_RETURN` 或没有
  `close_notify` 的 EOF → `Closed`；其它致命错误按 `Closed` 返回（链路不可用），
  原因写进 `last_error()`。**这样 `Client` 的"读到 `Closed` 就重连"逻辑不需要知道 TLS 存在。**
- `write()` 内部循环到写完或有界超时：允许 `SSL_write` 短写（`SSL_MODE_ENABLE_PARTIAL_WRITE`），
  `WANT_*` 时 `poll` 重试（`SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER`），超时返回
  `Error::Timeout`。保住 `Transport` 的 all-or-error 契约。

### 8.3 错误映射

| 情况 | `connect()` 返回 | `Client` 行为 |
|---|---|---|
| TCP 连不上 / 超时 | `Closed` / `Timeout` | 退避重试 |
| 证书链、主机名、有效期校验失败 | `CertVerify` | **默认判永久失败**（`State::Failed`），`retry_on_permanent` 可覆盖 |
| 其它握手/配置错误（协议不匹配、密钥加载失败…） | `Tls` | 退避重试（配置错误会一直重试，需看日志定位） |
| 读/写时链路不可用 | `Closed` | 重连 |

`Transport::last_error()` 提供人可读细节（OpenSSL 错误栈、`X509_verify_cert_error_string`、
主机名不匹配原因），`FailoverTransport` 汇报时会带上失败的端点。

### 8.4 验证方式

e2e 用 `e2e/make_certs.py` 在构建目录里**运行时生成**自签 CA / 服务端 / 客户端证书
（调用 `openssl` CLI；证书不入库，避免仓库躺私钥、也避免过期）。`mock_broker.py --tls`
做 TLS 服务端。场景：单向 TLS 收发、按 DNS 名连接（SNI + `set1_host`）、CA 不认 → 永久失败、
主机名不匹配 → 永久失败、mTLS 不给客户端证书连不上 / 给了成功。
**这些都在 WSL2 Ubuntu 24.04.1 + OpenSSL 3.0.13 上真实跑过（29/29）**；
Windows 侧未实现，CI 的 windows job 显式关 TLS。

## 9. 打包与可选性

- `find_package(Threads REQUIRED)` 是硬依赖（异步外壳与 CLI 都用到线程）。
- `-Wall -Wextra -Wpedantic -Werror` 只加在本项目的目标上（`PRIVATE`），下游不会继承 `-Werror`。
- 安装导出 `mqttcTargets` + `mqttcConfig.cmake`（含 `find_dependency(Threads)`；构建时带 TLS
  则还含 `find_dependency(OpenSSL)`），下游 `find_package(mqttc CONFIG REQUIRED)`
  然后链 `mqttc::mqttc` 即可。
- `MQTTC_ENABLE_TLS` **默认开**（锁定的范围）。Windows 上打开会在配置阶段明确报错——
  那边的 `TlsTransport` 还没实现，需要 MSYS2 + OpenSSL 环境来编写与验证。

## 10. 参考了什么

只读了这些实现的结构与错误处理思路，**没有整段搬运代码**：

- `eclipse/paho.mqtt.c`：报文构造与 keepalive 处理的分工（EPL-2.0 / EDL-1.0）。
- `redboltz/mqtt_cpp`：编解码组织的分层（Boost-1.0）。本项目不采用它的异步模型。
- `knolleary/pubsubclient`：精简客户端的最小骨架与取舍（MIT）。
- `jiejieTop/mqttclient`（GPL-2.0）：**只看状态机划分与重连流程的文字描述，不看代码细节**。
- `eclipse/mosquitto`：服务端行为（keepalive 判死、重连）用来对照。

## 11. 已知未做 / 未验证

**已在 Linux 上实测**（WSL2 Ubuntu 24.04.1，GCC 13.3.0 / Clang 18.1.3 / OpenSSL 3.0.13，
2026-11-17）：`src/transport_posix.cpp` 与 `src/tls_transport.cpp` 首次被真实编译并运行，
两套编译器零警告、`ctest` 各 2/2（e2e 含 TLS，29/29）；ASan + UBSan + LeakSanitizer 全绿；
**TSan 全绿**（`129 cases, 0 failed`，`ctest` 2/2）；覆盖率实测行 90.9% / 函数 99.3% /
分支 57.7%（含 TLS，`tls_transport` 61%）；两个 libFuzzer 目标各实跑 60 秒
（1181 万 / 1824 万次执行）无崩溃。TSan 需要 `sysctl vm.mmap_rnd_bits=28`——默认 32 时
TSan 的影子地址空间与高熵 ASLR 冲突，连用例都起不来（那是环境问题，不是竞争报告）。

**仍然未验证 / 未做**：

- **Windows 侧 TLS 未实现**：只做了 POSIX/OpenSSL 版；Windows + `MQTTC_ENABLE_TLS=ON`
  会在配置阶段明确报错。Windows 的明文构建（w64devkit，TLS OFF）真实跑过。
- GitHub Actions 工作流**从未推送、从未执行**，文档里的 CI 命令只是配置。
- 没有对拍：没跟真实 broker（mosquitto）做过系统性比对，只用了自带的 mock broker。
- 没有基准数字；`macOS` 与 `MSVC` 未验证，也不做支持承诺。
- 模糊测试只是短时随机搜索，不是"没有 bug"的证明。
- 保留消息、`$SYS` 主题、会话恢复、共享订阅、异步 IO（epoll/IOCP）都明确不做
  （见 README 的限制清单）。
