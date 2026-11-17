# 安全策略

## 这个库是什么

`mqttc` 是一个 MQTT 3.1.1 **客户端**（QoS 0/1）加一个小 CLI。它不实现 broker，也不管理服务端。
所以这里讨论的威胁模型是**客户端侧**的：对端（broker）或被中间人劫持的链路，送回来畸形或超大的
报文，试图让客户端崩溃、越界或耗尽资源。

反过来说，客户端是主动去连 broker 的一方，broker 地址与凭据都由调用方提供；本库不负责
"防止你连到坏 broker"。

## 现有的防护

对端的数据进到状态机之前会过这几道关卡：

- **单报文上限 `max_packet_bytes`**（`ClientOptions`，默认 16 MiB）。协议允许 Remaining Length
  到 256 MiB，但那是"对端说了算"的数字。读进来的字节数超过自己的上限，或解析出的报文长度超过
  上限，就判 `oversize`：断开连接、计数（`Stats::oversize_packets`）、走重连流程，不尝试去缓冲它。
- **长度字段的上限校验**：`peek_packet_size()` 只负责量长度，并对协议上限（`kMaxPacketSize`，
  256 MiB）与长度字段本身的合法性把关——Remaining Length 最多 4 字节，出现第 5 个字节直接判
  协议错误。此后调用方（`Client::drain_input`）再拿这个长度与本端 `max_packet_bytes` 比较，
  两个上限里更严的那个说了算，超了就在缓冲之前断掉。
- **严格 UTF-8 与主题校验**：字符串字段走 `wire::valid_utf8()`（拒绝过长编码、代理对、`U+0000`）；
  主题名与过滤器走 `topic` 层的完整校验（`#` / `+` 必须独占一层、`#` 只能殿后、主题名不含
  通配符）。任一不合法即判 `ProtocolError`。
- **逐字段的畸形报文拒绝**：固定头标志位、packet id 为 0、QoS 3、保留报文类型、多出字节等都会
  被 `decode()` 判 `ProtocolError`，用例见 `tests/test_packet.cpp`。
- **回调返回 false 走丢弃路径**：应用回调表示处理不过来时，该条被丢弃并计入 `Stats::dropped`，
  QoS 1 也照旧回 PUBACK（避免 broker 无限重发）。这是"实时性优先于完整性"的取舍，**不是可靠性
  队列**。
- **有界队列**：异步外壳（`AsyncClient`）的入站队列同时限条数（`inbound_queue_capacity`）与字节数
  （`inbound_max_bytes`）——一条超大消息也顶不穿内存；命令队列满了 `publish()` 立刻返回 `Busy`，
  由调用方决定退让。同步 `Client` 的未确认 QoS1 消息也有上限（`max_inflight`），到顶后
  `publish()` 返回 `Busy`。
- **有界的连接 / 发送超时**：`TcpTransport` 的 `connect_timeout_ms` / `send_timeout_ms` 默认有界，
  避免挂在黑洞地址或满窗口的对端上。
- **TLS 证书校验**（POSIX）：默认开启，校验 CA 链与主机名（DNS SAN / IP SAN）；
  校验失败映射 `Error::CertVerify` 并按永久失败处理，**不会自动降级成明文**。
  没有关闭校验的"隐藏开关"：只有调用方显式 `verify_peer=false` / CLI `--insecure` 才会关。

## 已知的薄弱点 / 未做的事

照实写，不美化：

- **TLS 只在 POSIX 上**。Linux 侧已实现并实测（OpenSSL 3，默认校验证书、支持 mTLS）；
  Windows 侧尚未实现，Windows 构建必须 `-DMQTTC_ENABLE_TLS=OFF`，此时链路是明文，
  不要用在需要保密的场景。
- **`--insecure` 会关闭证书校验**：CLI 会先打警告，但这意味着中间人可以冒充 broker。
  它只用于连接自签证书的测试环境。证书校验失败默认按永久失败处理（退出码 3），
  不会悄悄降级。
- **TLS 不做吊销检查**：只做标准证书链与主机名校验，不查 CRL / OCSP；证书被吊销后
  在过期前仍会被接受。
- **没有访问控制**：本库不做鉴权之外的任何授权判断，也不管 broker 的策略。能订什么主题、
  能发什么主题，取决于 broker 自己的 ACL。
- **模糊测试的时间很短**：`fuzz/fuzz_decode.cc` 与 `fuzz/fuzz_wire.cc` 已在 Linux
  （clang 18 + ASan/UBSan）上各实跑 60 秒——1181 万与 1824 万次执行，无崩溃、无泄漏报告。
  首轮撞出的两处问题都在 fuzzer 目标自己的不变量里（Remaining Length 的非最短编码），
  不在库里，已补单元回归用例。60 秒的随机搜索**不是**"没有漏洞"的证明；手工边界/畸形
  用例见 `tests/test_fuzz_lite.cpp` 与 `tests/test_packet.cpp`。
- **不保证抗 DoS**：对端持续以合法报文灌入时，本库只能靠队列上限丢弃，没有速率限制、没有背压、
  不区分"合法洪泛"和"正常流量"。客户端也不限制 broker 下发消息的速率。
- **单线程核心**：同步 `Client` 是单线程的，`pump()` 由调用方驱动，并发正确性由调用方负责。
  异步外壳用锁保护队列，已在 Linux 上跑过 TSan 且干净（129 个用例 + e2e，见 README
  "验证状态"），但这同样不构成对任意用法下无数据竞争的保证。

## 报告漏洞

请**不要**开公开 issue。通过本仓库的 GitHub 私有漏洞报告功能（Security → Report a
vulnerability），或直接联系仓库维护者。修复前请给一段合理的缓冲时间。

## 不在范围内

这些不会按漏洞处理：

- 需要物理接触目标设备的场景。
- 需要伪造受信任 CA 签名的证书、或攻破 CA 本身的场景。
- 明确要求 `--insecure` 或 TLS OFF 才建立明文连接的环境——那是调用方自己选择的边界。
- 社会工程学。
- 已经明确标注"不做"的功能缺口（Windows 侧 TLS、QoS 2、会话持久化等）：那是有意的边界，
  不是漏洞。
