#!/usr/bin/env python3
"""端到端验收脚本：真 socket + mock broker。

跑这些事：
1. mqttc sub / mqttc pub 对上 mock broker，消息真的收到；杀掉 broker 再起，
   客户端自己重连并重新订阅，第二条消息照样收到；
2. 两个 broker 的多端点故障转移：杀掉主用端点后切到备用端点并恢复收消息；
3. broker 回 CONNACK 4（凭据错）时判定为永久失败，CLI 以退出码 3 结束；
4. --tls（构建带 TLS 时由 CMake 传入）：TLS 握手与收发、CA 不认 / 主机名不匹配
   判永久失败、mTLS 需要客户端证书。

日志与运行时生成的证书落在构建目录（默认 build/e2e/），不写进源码目录。
用法：python e2e/run_e2e.py [--tls]
"""

import argparse
import os
import pathlib
import socket
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
BROKER = ROOT / "e2e" / "mock_broker.py"
MAKE_CERTS = ROOT / "e2e" / "make_certs.py"
DEFAULT_CLI = ROOT / "build" / ("mqttc.exe" if os.name == "nt" else "mqttc")

# 可被命令行覆盖（ctest 里会传 $<TARGET_FILE:mqttc_cli> 与自己的日志目录）
CLI = DEFAULT_CLI
WORK = ROOT / "build" / "e2e"
PORT = 18831
HOST = "127.0.0.1"
RUN_TLS = False

results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    mark = "OK  " if ok else "FAIL"
    print(f"[ {mark} ] {name}{('  ' + detail) if detail else ''}", flush=True)


def wait_port(port=None, timeout=5.0):
    port = PORT if port is None else port
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, port), 0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def start_broker(log_path, port=None, extra_args=()):
    port = PORT if port is None else port
    handle = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "--host", HOST, "--port", str(port), *extra_args],
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    return proc, handle


def wait_for_text(path, needle, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            text = path.read_text(encoding="utf-8", errors="replace")
            if needle in text:
                return True
        time.sleep(0.1)
    return False


def read_text(path):
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def run_cli(args, timeout=15):
    return subprocess.run(
        [str(CLI)] + args,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def run_tls_scenarios():
    """TLS 正例与反例。证书由 make_certs.py 在 WORK/certs 里现生成（不入库）。"""
    certs = WORK / "certs"
    made = subprocess.run(
        [sys.executable, str(MAKE_CERTS), "--dir", str(certs)],
        capture_output=True, encoding="utf-8", errors="replace",
    )
    check("生成 TLS 测试证书", made.returncode == 0,
          (made.stderr or made.stdout).strip().replace("\n", " "))
    if made.returncode != 0:
        return

    ca = certs / "ca.crt"
    wrong_ca = certs / "wrong_ca.crt"
    server_crt = certs / "server.crt"
    server_key = certs / "server.key"
    client_crt = certs / "client.crt"
    client_key = certs / "client.key"

    port_tls = PORT + 30
    tls_log = WORK / "tls_broker.log"
    tls_args = ["--tls", "--cert", str(server_crt), "--key", str(server_key)]
    broker, handle = start_broker(tls_log, port_tls, tls_args)
    if not wait_port(port_tls):
        check("TLS broker 起监听", False, f"见 {tls_log}")
        broker.terminate()
        broker.wait(timeout=5)
        handle.close()
        return
    check("TLS broker 起监听", True, f"{HOST}:{port_tls}")

    sub_log = WORK / "tls_sub.log"
    sub_handle = sub_log.open("w", encoding="utf-8")
    tls_flags = ["--tls", "--cafile", str(ca)]
    sub = subprocess.Popen(
        [str(CLI), "sub", "-t", "tls/#", "-H", HOST, "-p", str(port_tls), "-k", "5", *tls_flags],
        stdout=sub_handle,
        stderr=subprocess.STDOUT,
    )
    check("TLS sub 连上 broker", wait_for_text(sub_log, "已连接", 8), f"见 {sub_log}")

    pub = run_cli(["pub", "-t", "tls/a", "-m", "over-tls",
                   "-H", HOST, "-p", str(port_tls), *tls_flags])
    check("TLS 发布成功", pub.returncode == 0,
          pub.stderr.strip().replace("\n", " "))
    check("TLS 消息送达 sub", wait_for_text(sub_log, "tls/a over-tls", 5), f"见 {sub_log}")
    check("握手日志里有 TLS 完成", "TLS 握手完成" in read_text(tls_log))

    # 用 DNS 名连接：走 SNI + SSL_set1_host 这条路径（上面两次是 IP 字面量）
    by_name = run_cli(["pub", "-t", "tls/b", "-m", "by-name", "-H", "localhost",
                       "-p", str(port_tls), *tls_flags])
    check("按 DNS 名（localhost）连接并通过主机名校验", by_name.returncode == 0,
          by_name.stderr.strip().replace("\n", " "))

    sub.terminate()
    sub.wait(timeout=5)
    sub_handle.close()

    # 反例 1：CA 不在信任库里 → 证书校验失败，按永久失败退出（3）
    bad_ca = run_cli(["pub", "-t", "x/y", "-m", "hi", "-H", HOST, "-p", str(port_tls),
                      "--tls", "--cafile", str(wrong_ca)], timeout=20)
    check("CA 不认 → 永久失败并退出码 3",
          bad_ca.returncode == 3 and "证书校验失败" in bad_ca.stderr,
          f"退出码 {bad_ca.returncode}: {bad_ca.stderr.strip().replace(chr(10), ' ')}")

    # 反例 2：主机名不匹配（证书 SAN 是 localhost/127.0.0.1，这里故意要别的名字）
    bad_name = run_cli(["pub", "-t", "x/y", "-m", "hi", "-H", HOST, "-p", str(port_tls),
                        "--tls", "--cafile", str(ca), "--sni", "wrong.example"], timeout=20)
    check("主机名不匹配 → 永久失败并退出码 3",
          bad_name.returncode == 3 and "证书校验失败" in bad_name.stderr,
          f"退出码 {bad_name.returncode}: {bad_name.stderr.strip().replace(chr(10), ' ')}")

    broker.terminate()
    broker.wait(timeout=5)
    handle.close()

    # ---- mTLS：服务端要求客户端证书 ----
    port_mtls = PORT + 33
    mtls_log = WORK / "mtls_broker.log"
    mtls_args = tls_args + ["--require-client-cert", "--cafile", str(ca)]
    mtls_broker, mtls_handle = start_broker(mtls_log, port_mtls, mtls_args)
    if not wait_port(port_mtls):
        check("mTLS broker 起监听", False, f"见 {mtls_log}")
        mtls_broker.terminate()
        mtls_broker.wait(timeout=5)
        mtls_handle.close()
        return
    check("mTLS broker 起监听", True, f"{HOST}:{port_mtls}")

    no_cert = run_cli(["pub", "-t", "x/y", "-m", "hi", "-H", HOST, "-p", str(port_mtls),
                       "--tls", "--cafile", str(ca)], timeout=20)
    check("mTLS 不给客户端证书 → 连不上（退出码 2）", no_cert.returncode == 2,
          f"退出码 {no_cert.returncode}: {no_cert.stderr.strip().replace(chr(10), ' ')}")

    with_cert = run_cli(["pub", "-t", "mtls/a", "-m", "with-cert", "-H", HOST,
                         "-p", str(port_mtls), "--tls", "--cafile", str(ca),
                         "--cert", str(client_crt), "--key", str(client_key)], timeout=20)
    check("mTLS 带客户端证书 → 发布成功", with_cert.returncode == 0,
          with_cert.stderr.strip().replace("\n", " "))

    mtls_broker.terminate()
    mtls_broker.wait(timeout=5)
    mtls_handle.close()
    print(f"\n---- TLS broker（{tls_log}）----")
    print(read_text(tls_log).strip())


def parse_args(argv):
    parser = argparse.ArgumentParser(description="mqttc 端到端验收")
    parser.add_argument("--binary", type=pathlib.Path, default=DEFAULT_CLI,
                        help="mqttc 可执行文件路径（默认 build/mqttc[.exe]）")
    parser.add_argument("--port", type=int, default=18831, help="mock broker 监听端口")
    parser.add_argument("--work-dir", type=pathlib.Path, default=ROOT / "build" / "e2e",
                        help="日志目录（不写进源码目录）")
    parser.add_argument("--tls", action="store_true",
                        help="额外跑 TLS 场景（构建带 TLS 时由 CMake 传这个参数）")
    return parser.parse_args(argv)


def main(argv=None):
    global CLI, WORK, PORT, RUN_TLS

    args = parse_args(argv)
    CLI = args.binary
    WORK = args.work_dir
    PORT = args.port
    RUN_TLS = args.tls

    # Windows 控制台/管道默认是 GBK，日志里有中文，统一按 UTF-8 输出
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError):
            pass

    if not CLI.exists():
        print(f"找不到 {CLI}，先跑 cmake --build build", file=sys.stderr)
        return 2
    WORK.mkdir(parents=True, exist_ok=True)

    broker_log = WORK / "broker1.log"
    broker_log_2 = WORK / "broker2.log"
    sub_log = WORK / "sub.log"

    broker, broker_handle = start_broker(broker_log)
    if not wait_port():
        check("mock broker 起监听", False, f"见 {broker_log}")
        broker.terminate()
        return 2
    check("mock broker 起监听", True, f"{HOST}:{PORT}")

    sub_handle = sub_log.open("w", encoding="utf-8")
    sub = subprocess.Popen(
        [str(CLI), "sub", "-t", "demo/#", "-H", HOST, "-p", str(PORT), "-k", "5"],
        stdout=sub_handle,
        stderr=subprocess.STDOUT,
    )
    check("mqttc sub 连上 broker", wait_for_text(sub_log, "已连接", 5), f"见 {sub_log}")

    first = run_cli(["pub", "-t", "demo/a", "-m", "hello", "-H", HOST, "-p", str(PORT), "-q", "1"])
    check(
        "mqttc pub -t demo/a -m hello（QoS 1）",
        first.returncode == 0,
        first.stderr.strip().replace("\n", " "),
    )
    check(
        "sub 收到 hello",
        wait_for_text(sub_log, "demo/a hello", 5),
        f"见 {sub_log}",
    )

    # 断线自愈：杀掉 broker，等客户端察觉，再把 broker 起回来
    broker.terminate()
    broker.wait(timeout=5)
    broker_handle.close()
    time.sleep(1.5)

    broker, broker_handle = start_broker(broker_log_2)
    if not wait_port():
        check("broker 重启", False)
    else:
        check("broker 重启", True)

    check("客户端自动重连", wait_for_text(sub_log, "已重连", 25), f"见 {sub_log}")

    second = run_cli(["pub", "-t", "demo/b", "-m", "again", "-H", HOST, "-p", str(PORT)])
    check(
        "重连后 mqttc pub -t demo/b -m again",
        second.returncode == 0,
        second.stderr.strip().replace("\n", " "),
    )
    check("重连后 sub 仍能收到", wait_for_text(sub_log, "demo/b again", 5))
    check("重连后重新订阅过", "subscribe 'demo/#'" in read_text(broker_log_2))

    sub.terminate()
    sub.wait(timeout=5)
    sub_handle.close()
    broker.terminate()
    broker.wait(timeout=5)
    broker_handle.close()

    # ---- 场景 B：多端点故障转移（两个 broker，杀掉主用那个）----
    port_primary = PORT + 10
    port_standby = PORT + 11
    failover_log = WORK / "failover.log"
    primary_log = WORK / "failover_primary.log"
    standby_log = WORK / "failover_standby.log"

    primary, primary_handle = start_broker(primary_log, port_primary)
    standby, standby_handle = start_broker(standby_log, port_standby)
    if not (wait_port(port_primary) and wait_port(port_standby)):
        check("两个 broker 起监听", False)
    else:
        check("两个 broker 起监听", True, f"{port_primary} / {port_standby}")

    failover_handle = failover_log.open("w", encoding="utf-8")
    failover = subprocess.Popen(
        [str(CLI), "sub", "-t", "failover/#", "-k", "5",
         "-H", f"{HOST}:{port_primary},{HOST}:{port_standby}"],
        stdout=failover_handle,
        stderr=subprocess.STDOUT,
    )
    check("多端点 sub 连上主用端点", wait_for_text(failover_log, f"已连接 {HOST}:{port_primary}", 5),
          f"见 {failover_log}")

    via_primary = run_cli(["pub", "-t", "failover/a", "-m", "via-primary",
                           "-H", HOST, "-p", str(port_primary)])
    check("经主用端点发布并收到", via_primary.returncode == 0
          and wait_for_text(failover_log, "failover/a via-primary", 5))

    primary.terminate()
    primary.wait(timeout=5)
    primary_handle.close()

    check("主用端点挂掉后切到备用端点", wait_for_text(failover_log, "已切换到备用端点", 25),
          f"见 {failover_log}")

    via_standby = run_cli(["pub", "-t", "failover/b", "-m", "via-standby",
                           "-H", HOST, "-p", str(port_standby)])
    check("经备用端点发布并收到", via_standby.returncode == 0
          and wait_for_text(failover_log, "failover/b via-standby", 5))
    check("切换后在新端点上重新订阅过", "subscribe 'failover/#'" in read_text(standby_log))

    failover.terminate()
    failover.wait(timeout=5)
    failover_handle.close()
    standby.terminate()
    standby.wait(timeout=5)
    standby_handle.close()

    # ---- 场景 C：永久失败以非零码退出（broker 回 CONNACK 4）----
    port_reject = PORT + 20
    reject_log = WORK / "reject.log"
    reject_broker, reject_handle = start_broker(reject_log, port_reject, ["--reject-code", "4"])
    if not wait_port(port_reject):
        check("拒绝型 broker 起监听", False, f"见 {reject_log}")
    else:
        check("拒绝型 broker 起监听", True, f"{port_reject}")

    rejected = run_cli(["pub", "-t", "x/y", "-m", "hi", "-H", HOST, "-p", str(port_reject)],
                       timeout=20)
    check("CONNACK 4（凭据错）判定为永久失败并退出码 3",
          rejected.returncode == 3 and "永久失败" in rejected.stderr,
          f"退出码 {rejected.returncode}: {rejected.stderr.strip().replace(chr(10), ' ')}")

    reject_broker.terminate()
    reject_broker.wait(timeout=5)
    reject_handle.close()

    # ---- 场景 D：TLS（构建带 TLS 时才跑）----
    if RUN_TLS:
        run_tls_scenarios()

    print(f"\n---- mqttc sub 输出（{sub_log}）----")
    print(read_text(sub_log).strip())
    print(f"\n---- 第一个 broker（{broker_log}）----")
    print(read_text(broker_log).strip())
    print(f"\n---- 重启后的 broker（{broker_log_2}）----")
    print(read_text(broker_log_2).strip())
    print(f"\n---- 故障转移 sub 输出（{failover_log}）----")
    print(read_text(failover_log).strip())
    print(f"\n---- 备用端点的 broker（{standby_log}）----")
    print(read_text(standby_log).strip())

    failed = [name for name, ok in results if not ok]
    print()
    if failed:
        print(f"{len(results) - len(failed)}/{len(results)} 项通过，失败: {', '.join(failed)}")
        return 1
    print(f"{len(results)}/{len(results)} 项通过")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
