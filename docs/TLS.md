# TLS

TLS 是 `Transport` 的另一种实现（`TlsTransport`），所以 `Client`、`AsyncClient`、
`FailoverTransport` 的用法完全不变；换掉传输层对象即可。实现只在 POSIX 上
（Linux/macOS 的 OpenSSL 3 路径），Windows 侧尚未实现。

## 构建

TLS 默认开启（`MQTTC_ENABLE_TLS=ON`），需要 OpenSSL 开发包：

```sh
# Debian / Ubuntu
sudo apt install -y libssl-dev
cmake -S . -B build && cmake --build build
```

没有 OpenSSL 的环境显式关闭：

```sh
cmake -S . -B build -DMQTTC_ENABLE_TLS=OFF
```

Windows 目前必须关闭（`-DMQTTC_ENABLE_TLS=OFF`）；打开会在 CMake 配置阶段明确报错，
原因是没有 Windows 侧的 `TlsTransport` 实现。

## 选项（`TlsOptions`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `ca_file` | 空 | CA 证书文件（PEM）。与 `ca_path` 都空时用 OpenSSL 系统默认信任库 |
| `ca_path` | 空 | CA 证书目录 |
| `cert_file` | 空 | 客户端证书（mTLS）。填了就必须同时填 `key_file` |
| `key_file` | 空 | 客户端私钥（PEM） |
| `server_name` | 空 | SNI 与主机名校验用的名字。空则用连接的 host |
| `verify_peer` | `true` | 校验服务端证书。**关掉就没有身份认证**；只允许显式关 |
| `min_tls_version` | `"1.2"` | 最低协议版本，支持 `"1.2"` / `"1.3"` |
| `handshake_timeout_ms` | 10000 | 握手总预算（不含 TCP 连接） |

TCP 连接本身的超时来自同一次构造里传入的 `TcpTransportOptions`
（`connect_timeout_ms` / `send_timeout_ms`）。

## CLI

```sh
# 单向 TLS（只校验服务端）
mqttc sub -t 'demo/#' -H broker.example -p 8883 --tls --cafile ca.crt

# 主机名与 -H 不一致时（例如用 IP 连、证书却是 DNS 名）
mqttc pub -t demo/a -m hello -H 203.0.113.10 -p 8883 \
    --tls --cafile ca.crt --sni broker.example

# mTLS
mqttc pub -t demo/a -m hello -H broker.example \
    --tls --cafile ca.crt --cert client.crt --key client.key

# 连接自签证书的测试 broker，不校验（只用于实验；CLI 会打印警告）
mqttc sub -t '#' -H 127.0.0.1 -p 8883 --tls --insecure
```

`--tls` 且没给 `-p` 时默认端口是 8883。`-H` 仍可给多端点，TLS 对每个端点生效
（证书校验名默认取该端点的 host）。

## 错误语义

| 情况 | 传输层返回 | 上层行为 |
|---|---|---|
| TCP 连不上 / 连接超时 | `Closed` / `Timeout` | 退避重试（多端点时换下一个） |
| 证书链不认、主机名不匹配、证书过期 | `CertVerify` | **默认永久失败**（`State::Failed`）；`retry_on_permanent` 可覆盖 |
| 协议版本不匹配、密钥加载失败等配置/握手错误 | `Tls` | 按普通失败退避重试（配置错误通常需要人看日志） |
| 已连接后读写发现链路不可用 | `Closed` | 重连 |

具体原因不在枚举里：`TlsTransport::last_error()` 返回人可读串（OpenSSL 错误栈、
`X509_verify_cert_error_string` 的结果）。`FailoverTransport::last_error()` 会说明是哪
个端点失败的。CLI 把这两者拼进 stderr，永久失败时退出码是 3。

`read()` 不阻塞；`write()` 内部循环到写完或有界超时，保持 `Transport` 的 all-or-error
契约。这两条是 `Client` 不必知道 TLS 存在的前提。

## 验证状态（照实写）

**已在 WSL2 Ubuntu 24.04.1 + OpenSSL 3.0.13 上真实跑过**（2026-11-17）：

- GCC 13.3.0 与 Clang 18.1.3 构建零警告；`ctest` 2/2。
- ASan + UBSan + LeakSanitizer、TSan 全绿（TLS 代码一并插桩；TSan 需 `vm.mmap_rnd_bits=28`）。
- e2e 29/29：单向 TLS 收发、按 DNS 名连接（SNI + 主机名校验）、CA 不认 → 退出码 3、
  主机名不匹配 → 退出码 3、mTLS 不给客户端证书连不上 / 给证书成功。
- 覆盖率里 `tls_transport.cpp` 行覆盖 61%：大量配置错误与超时分支没有用例走到，
  这个数字不修饰。

**没有验证的**：

- Windows 实现不存在，也没有在 Windows 上跑过任何 TLS 连接。
- 没有与真实 broker（如 mosquitto 的 TLS 端口）做过互操作测试；只用了自带的 Python
  `ssl` 服务端。
- macOS 理论上走同一份 POSIX 代码，但没有环境，未验证。
- 没有做过证书轮换、吊销（CRL/OCSP）相关的事；`verify_peer` 只做标准链校验，
  **不做 OCSP stapling、不做 CRL 检查**。
- `min_tls_version = "1.3"` 的配置分支没有单独的 e2e 用例（e2e 全走默认的 1.2 下限）；
  代码路径就是一次 `SSL_CTX_set_min_proto_version()`，但没有实跑过。

## 已知边界

- 只支持 TLS 1.2 / 1.3，默认最低 1.2。
- 不做会话恢复（session resumption）；每次重连完整握手。
- 客户端证书只支持单个证书链文件 + 单个私钥文件（PEM）。
- 不做 ALPN。