"""在 Linux 上使用伪终端和本地 Mosquitto 做端到端验证。"""

import argparse
import json
import os
from pathlib import Path
import pty
import select
import socket
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import urllib.request
from contextlib import ExitStack

TOPIC = "stm32"
PROJECT_DIR = Path(__file__).resolve().parents[1]


def crc_frame(data):
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0xA001 if crc & 1 else 0)
    return data + struct.pack("<H", crc)


def wait_for(predicate, description, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"等待超时: {description}")


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return str(sock.getsockname()[1])


def simulate(master, stop, errors):
    pending = bytearray()
    count = 0
    try:
        while not stop.is_set():
            if not select.select([master], [], [], 0.1)[0]:
                continue
            pending.extend(os.read(master, 256))
            while len(pending) >= 8:
                request = bytes(pending[:8])
                del pending[:8]
                slave = request[0]
                assert slave in (1, 2) and request == crc_frame(bytes((slave, 3, 0, slave, 0, 2)))
                if slave == 1:
                    count += 1
                    data = struct.pack(">HH", 230 + count, 567)
                else:
                    data = struct.pack(">I", 123456)
                frame = crc_frame(bytes((slave, 3, 4)) + data)
                if slave == 2 and count == 1:
                    frame = frame[:-1] + bytes([frame[-1] ^ 1])
                os.write(master, frame)
    except Exception as error:
        errors.append(error)


def stop_child(child):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=6)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            raise


def query(database, sql):
    if not database.exists():
        return []
    with sqlite3.connect(database) as db:
        try:
            return db.execute(sql).fetchall()
        except sqlite3.OperationalError as error:
            if "no such table" in str(error):
                return []
            raise


def check(root, verify_watchdog):
    database = root / "gateway.sqlite3"
    port, ui_port = unused_port(), unused_port()
    while ui_port == port:
        ui_port = unused_port()
    notification_path = root / "notify.sock"
    master, terminal = pty.openpty()
    stop = threading.Event()
    errors = []
    simulator = threading.Thread(target=simulate, args=(master, stop, errors))
    simulator.start()

    with ExitStack() as cleanup:
        cleanup.callback(os.close, master)
        cleanup.callback(os.close, terminal)
        cleanup.callback(simulator.join)
        cleanup.callback(stop.set)
        if verify_watchdog:
            notifications = cleanup.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM))
            notifications.bind(str(notification_path))
            notifications.settimeout(5)

        def launch(command, **options):
            child = subprocess.Popen(command, **options)
            cleanup.callback(stop_child, child)
            return child

        def start_gateway():
            env = {**os.environ, "GATEWAY_MQTT_CLIENT_ID": "test-gateway"}
            if verify_watchdog:
                env["NOTIFY_SOCKET"] = str(notification_path)
            return launch([str(PROJECT_DIR / "edge-gateway"), os.ttyname(terminal), str(database),
                           "127.0.0.1", port, TOPIC], env=env)

        def cached():
            return [payload for (payload,) in query(database, "SELECT payload FROM samples ORDER BY id")]

        def start_broker(name):
            log_path = root / name
            with log_path.open("w") as log:
                broker = launch(["mosquitto", "-p", port, "-v"], stderr=log)
            wait_for(lambda: " running" in log_path.read_text(), "本地 MQTT broker 启动")
            return broker, log_path

        gateway = start_gateway()
        if verify_watchdog:
            assert notifications.recv(256) == b"READY=1"
            assert notifications.recv(256) == b"WATCHDOG=1"

        wait_for(lambda: len(cached()) >= 7, "离线缓存至少七条")
        stop_child(gateway)
        assert gateway.returncode == 0
        saved = cached()
        assert saved[0] == '{"temperature":23.1,"shidu":56.7,"light":0}'
        assert saved[1] == '{"temperature":23.2,"shidu":56.7,"light":123}'

        broker, broker_log = start_broker("broker-first.log")
        subscriber = launch(["mosquitto_sub", "-h", "127.0.0.1", "-p", port,
                             "-t", TOPIC, "-q", "1", "-C", str(len(saved) + 1)],
                            stdout=subprocess.PIPE, text=True)
        wait_for(lambda: "Sending SUBACK" in broker_log.read_text(), "监听者订阅主题")
        gateway = start_gateway()
        output, _ = subscriber.communicate(timeout=15)
        received = output.splitlines()
        assert received[:len(saved)] == saved
        assert received[-1].startswith('{{"temp":') and received[-1].endswith(',"humi":56.7,"light":123}}')
        wait_for(lambda: not cached(), "PUBACK 后清空缓存")

        launch(["python3", str(PROJECT_DIR / "ui/server.py"), str(database), "--port", ui_port])
        page_url = f"http://127.0.0.1:{ui_port}"
        client = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def monitor(path):
            try:
                with client.open(page_url + path, timeout=1) as response:
                    return response.read()
            except OSError:
                return None

        page = wait_for(lambda: monitor("/"), "监控页面启动")
        assert "边缘网关监控".encode() in page
        status = json.loads(monitor("/api/status"))
        assert status["state"]["light"] == 123 and status["state"]["mqtt"] == 1
        assert isinstance(status["pending"], int)
        assert status["state"]["temperature_online"] and status["state"]["light_online"]
        assert status["logs"][0]["ts"] >= status["logs"][-1]["ts"]

        stop_child(broker)
        wait_for(lambda: len(cached()) >= 2, "断线重新缓存")
        wait_for(lambda: json.loads(monitor("/api/status"))["state"]["mqtt"] == 0,
                 "页面显示 MQTT 离线")
        start_broker("broker-reconnect.log")
        wait_for(lambda: not cached(), "未重启网关情况下重连补传")
        assert gateway.poll() is None and not errors, errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-watchdog", action="store_true",
                        help="在禁止 UNIX 数据报套接字的容器中跳过 systemd 通知检查")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as path:
        check(Path(path), not args.skip_watchdog)
    if not args.skip_watchdog:
        print("PASS: systemd 看门狗通知")
    print("PASS: RTU、CRC 拒收、MQTT 直报/跨批补传/重连、监控 API")
