#!/usr/bin/env python3
"""脚本化 MQTT 3.1.1 broker，只满足端到端测试需要。

不是产品，也没有打算是：不做鉴权、不做 QoS2、不做保留消息、不做离线队列，
不保证任何并发正确性之外的东西。它的用途是让 mqttc 在真 socket 上跑一趟。

支持：CONNECT / PUBLISH（QoS 0、1）/ SUBSCRIBE / UNSUBSCRIBE / PINGREQ / DISCONNECT；
可选 TLS（--tls，证书用 e2e/make_certs.py 现生成）与客户端证书校验（mTLS）。
"""

import argparse
import socket
import ssl
import struct
import sys
import threading

def log(message):
    print(message, flush=True)


def use_utf8_output():
    """Windows 上 stdout 被重定向时默认是 GBK，日志里全是中文，统一成 UTF-8。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError):
            pass


def read_exact(conn, count):
    buf = b""
    while len(buf) < count:
        chunk = conn.recv(count - len(buf))
        if not chunk:
            raise ConnectionError("对端关闭")
        buf += chunk
    return buf


def read_varint(conn):
    multiplier = 1
    value = 0
    for _ in range(4):
        byte = read_exact(conn, 1)[0]
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value
        multiplier *= 128
    raise ValueError("Remaining Length 超过 4 字节")


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        out.append(byte)
        if not value:
            return bytes(out)


def read_packet(conn):
    first = read_exact(conn, 1)[0]
    length = read_varint(conn)
    body = read_exact(conn, length) if length else b""
    return first, body


def read_utf8(body, pos):
    size = struct.unpack_from("!H", body, pos)[0]
    pos += 2
    return body[pos:pos + size].decode("utf-8"), pos + size


def topic_matches(topic_filter, topic):
    """与 mqttc 的语义一致：'#' 匹配父层与任意层子级（'a/#' 匹配 'a'），
    首层通配符不匹配 '$' 开头的主题。"""
    levels = topic_filter.split("/")
    topic_levels = topic.split("/")
    if topic.startswith("$") and levels[0] in ("+", "#"):
        return False
    index = 0
    while index < len(levels):
        if levels[index] == "#":
            return True
        if index >= len(topic_levels):
            return False
        if levels[index] != "+" and levels[index] != topic_levels[index]:
            return False
        index += 1
    return index == len(topic_levels)


class Broker:
    def __init__(self):
        self.lock = threading.Lock()
        self.subscriptions = {}  # conn -> {filter: qos}
        self.next_packet_id = 1
        self.reject_code = None  # 非 None 时对所有 CONNECT 回这个返回码

    def add_subscription(self, conn, topic_filter, qos):
        with self.lock:
            self.subscriptions.setdefault(conn, {})[topic_filter] = qos

    def remove_subscription(self, conn, topic_filter):
        with self.lock:
            entries = self.subscriptions.get(conn)
            if entries:
                entries.pop(topic_filter, None)

    def drop_connection(self, conn):
        with self.lock:
            self.subscriptions.pop(conn, None)

    def take_targets(self, topic, qos):
        with self.lock:
            targets = []
            for conn, entries in self.subscriptions.items():
                matched = False
                grant = 0
                for topic_filter, entry_qos in entries.items():
                    if topic_matches(topic_filter, topic):
                        matched = True
                        grant = max(grant, entry_qos)
                if matched:
                    targets.append((conn, min(grant, qos)))
            return targets

    def allocate_packet_id(self):
        with self.lock:
            packet_id = self.next_packet_id
            self.next_packet_id = 1 if self.next_packet_id >= 65535 else self.next_packet_id + 1
            return packet_id

    def route(self, topic, payload, qos):
        topic_bytes = topic.encode("utf-8")
        for conn, deliver_qos in self.take_targets(topic, qos):
            body = struct.pack("!H", len(topic_bytes)) + topic_bytes
            if deliver_qos:
                body += struct.pack("!H", self.allocate_packet_id())
            body += payload
            first = bytes([0x30 | (deliver_qos << 1)])
            try:
                conn.sendall(first + encode_varint(len(body)) + body)
            except OSError:
                self.drop_connection(conn)


def handle_publish(conn, first, body):
    flags = first & 0x0F
    qos = (flags >> 1) & 0x03
    topic, pos = read_utf8(body, 0)
    packet_id = None
    if qos:
        packet_id = struct.unpack_from("!H", body, pos)[0]
        pos += 2
    payload = body[pos:]
    if qos == 1 and packet_id is not None:
        conn.sendall(b"\x40\x02" + struct.pack("!H", packet_id))
    return topic, payload, qos


def handle_subscribe(conn, broker, body):
    packet_id = struct.unpack_from("!H", body, 0)[0]
    pos = 2
    granted = []
    while pos < len(body):
        topic_filter, pos = read_utf8(body, pos)
        requested = body[pos]
        pos += 1
        grant = min(requested, 1)  # 这个 broker 只做 QoS 0/1
        broker.add_subscription(conn, topic_filter, grant)
        granted.append(grant)
        log(f"    subscribe {topic_filter!r} qos={grant}")
    payload = struct.pack("!H", packet_id) + bytes(granted)
    conn.sendall(bytes([0x90]) + encode_varint(len(payload)) + payload)


def handle_unsubscribe(conn, broker, body):
    packet_id = struct.unpack_from("!H", body, 0)[0]
    pos = 2
    while pos < len(body):
        topic_filter, pos = read_utf8(body, pos)
        broker.remove_subscription(conn, topic_filter)
        log(f"    unsubscribe {topic_filter!r}")
    conn.sendall(b"\xb0\x02" + struct.pack("!H", packet_id))


def serve(conn, address, broker, tls_context=None):
    if tls_context is not None:
        try:
            conn = tls_context.wrap_socket(conn, server_side=True)
        except (ssl.SSLError, OSError) as error:
            log(f"TLS 握手失败: {error}")
            conn.close()
            return
        log(f"TLS 握手完成 {address[0]}:{address[1]}")
    log(f"connect from {address[0]}:{address[1]}")
    try:
        while True:
            first, body = read_packet(conn)
            packet_type = first >> 4

            if packet_type == 1:
                if broker.reject_code is not None:
                    # 用来验证"永久失败"那条路径：直接回返回码然后断开
                    conn.sendall(bytes([0x20, 0x02, 0x00, broker.reject_code]))
                    log(f"    CONNACK rejected code={broker.reject_code}")
                    break
                # 只认协议版本之外的东西一概忽略，直接回“接受”
                conn.sendall(b"\x20\x02\x00\x00")
                log("    CONNACK sent")
            elif packet_type == 3:
                topic, payload, qos = handle_publish(conn, first, body)
                log(f"    publish {topic!r} qos={qos} payload={payload!r}")
                broker.route(topic, payload, qos)
            elif packet_type == 4:
                # 客户端对我们下发的 QoS1 消息回 PUBACK。必须处理：以前这里落到
                # else 分支被当成非法报文断开，QoS1 下行路径就没法在 e2e 里验证。
                packet_id = struct.unpack_from("!H", body, 0)[0]
                log(f"    PUBACK {packet_id}")
            elif packet_type == 8:
                handle_subscribe(conn, broker, body)
            elif packet_type == 10:
                handle_unsubscribe(conn, broker, body)
            elif packet_type == 12:
                conn.sendall(b"\xd0\x00")
            elif packet_type == 14:
                log("    DISCONNECT")
                break
            else:
                log(f"    unexpected packet type {packet_type}，断开")
                break
    except (ConnectionError, OSError, ValueError) as error:
        log(f"connection closed: {error}")
    finally:
        broker.drop_connection(conn)
        conn.close()


def main():
    use_utf8_output()
    parser = argparse.ArgumentParser(description="mqttc 端到端测试用的脚本化 broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--reject-code", type=int, default=None,
                        help="对所有 CONNECT 回这个返回码并断开（验证永久失败路径）")
    parser.add_argument("--tls", action="store_true", help="在 accept 后做 TLS 握手")
    parser.add_argument("--cert", default=None, help="服务端证书（PEM，--tls 时必填）")
    parser.add_argument("--key", default=None, help="服务端私钥（PEM，--tls 时必填）")
    parser.add_argument("--cafile", default=None,
                        help="校验客户端证书用的 CA（配合 --require-client-cert）")
    parser.add_argument("--require-client-cert", action="store_true",
                        help="要求客户端提供证书（mTLS）")
    args = parser.parse_args()

    tls_context = None
    if args.tls:
        if not args.cert or not args.key:
            parser.error("--tls 需要同时给 --cert 与 --key")
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(args.cert, args.key)
        if args.require_client_cert:
            if not args.cafile:
                parser.error("--require-client-cert 需要 --cafile 来校验客户端证书")
            tls_context.load_verify_locations(args.cafile)
            tls_context.verify_mode = ssl.CERT_REQUIRED

    broker = Broker()
    broker.reject_code = args.reject_code
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(16)
    log(f"mock broker 监听 {args.host}:{args.port}（Ctrl+C 退出）")

    try:
        while True:
            conn, address = server.accept()
            threading.Thread(target=serve,
                             args=(conn, address, broker, tls_context),
                             daemon=True).start()
    except KeyboardInterrupt:
        log("退出")
    finally:
        server.close()


if __name__ == "__main__":
    sys.exit(main())
