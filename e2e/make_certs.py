#!/usr/bin/env python3
"""在构建目录里生成端到端测试用的自签证书（不入库）。

为什么运行时生成：
- 仓库里不该躺私钥（会被密钥扫描器误报，也没有任何好处）；
- 证书有有效期，入库的测试证书迟早会过期，变成"某天 CI 突然红了"。

生成内容（全部落在 --dir 下）：
- ca.crt/ca.key           测试用 CA
- wrong_ca.crt/wrong_ca.key  另一个 CA（验证"CA 不认"这条反例）
- server.crt/server.key   服务端证书，SAN = DNS:localhost, IP:127.0.0.1
- client.crt/client.key   客户端证书（mTLS 用）

用法：python e2e/make_certs.py --dir build/e2e/certs
"""

import argparse
import pathlib
import shutil
import subprocess
import sys


def run(command):
    result = subprocess.run(command, capture_output=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(f"命令失败: {' '.join(command)}\n{result.stderr.strip()}")
    return result


def make_ca(directory, name, subject):
    run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-keyout", str(directory / f"{name}.key"),
        "-out", str(directory / f"{name}.crt"),
        "-subj", subject,
    ])


def sign(directory, name, ca_name, extensions):
    ext_file = directory / f"{name}.ext"
    ext_file.write_text(extensions, encoding="utf-8")
    run([
        "openssl", "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(directory / f"{name}.key"),
        "-out", str(directory / f"{name}.csr"),
        "-subj", f"/CN={name}",
    ])
    run([
        "openssl", "x509", "-req", "-days", "2",
        "-in", str(directory / f"{name}.csr"),
        "-CA", str(directory / f"{ca_name}.crt"),
        "-CAkey", str(directory / f"{ca_name}.key"),
        "-CAcreateserial",
        "-out", str(directory / f"{name}.crt"),
        "-extfile", str(ext_file),
    ])


def main(argv=None):
    parser = argparse.ArgumentParser(description="生成 mqttc e2e 用的自签证书")
    parser.add_argument("--dir", type=pathlib.Path, required=True, help="证书输出目录")
    args = parser.parse_args(argv)

    if shutil.which("openssl") is None:
        print("找不到 openssl 命令，无法生成 TLS 测试证书", file=sys.stderr)
        return 2

    directory = args.dir
    directory.mkdir(parents=True, exist_ok=True)

    # 每次都重新生成：RSA 2048 × 4 把钥匙在一秒量级，换来"永远不会用到过期证书"
    try:
        make_ca(directory, "ca", "/CN=mqttc e2e CA")
        make_ca(directory, "wrong_ca", "/CN=mqttc e2e wrong CA")
        sign(directory, "server", "ca",
             "basicConstraints=CA:FALSE\n"
             "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
             "extendedKeyUsage=serverAuth\n")
        sign(directory, "client", "ca",
             "basicConstraints=CA:FALSE\n"
             "extendedKeyUsage=clientAuth\n")
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1

    print(f"证书已生成到 {directory}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))