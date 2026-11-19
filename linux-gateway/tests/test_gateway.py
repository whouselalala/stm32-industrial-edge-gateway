"""虚拟串口回归测试与可选的 Mosquitto 端到端测试。"""

import json
import os
import pty
import select
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import paho.mqtt.client as mqtt

from modbus import ModbusError, ModbusRTU, crc16

ROOT = Path(__file__).resolve().parents[1]
BROKER = os.environ.get("MOSQUITTO") or shutil.which("mosquitto")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def until(check, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"条件在 {seconds} 秒内未满足")


def terminate(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise


class SerialTests(unittest.TestCase):
    def test_crc_and_invalid_frame(self):
        request = bytes.fromhex("01 03 00 01 00 02")
        self.assertEqual(crc16(request), 0xCB95)
        master, slave = pty.openpty()
        self.addCleanup(os.close, master)
        self.addCleanup(os.close, slave)
        reader = ModbusRTU(os.ttyname(slave), 9600, 0.3, False)
        reader.open()
        self.addCleanup(reader.close)
        received = []

        def device():
            if select.select([master], [], [], 1)[0]:
                received.append(os.read(master, 8))
                os.write(master, bytes.fromhex("01 03 04 00 EB 02 27 00 00"))

        writer = threading.Thread(target=device)
        writer.start()
        try:
            with self.assertRaisesRegex(ModbusError, "CRC16"):
                reader.read_pair(1, 1)
        finally:
            writer.join()
        self.assertEqual(received, [request + bytes.fromhex("95 CB")])
        with self.assertRaisesRegex(ModbusError, "超时"):
            reader.read_pair(1, 1)


@unittest.skipUnless(BROKER, "需要 Mosquitto 来运行端到端测试")
class GatewayTests(unittest.TestCase):
    def test_rs485_http_store_and_forward_across_restart(self):
        master, slave = pty.openpty()
        self.addCleanup(os.close, master)
        self.addCleanup(os.close, slave)
        stop = threading.Event()
        requests = []

        def sensors():
            buffer = b""
            while not stop.is_set():
                if not select.select([master], [], [], 0.05)[0]:
                    continue
                buffer += os.read(master, 64)
                while len(buffer) >= 8:
                    request, buffer = buffer[:8], buffer[8:]
                    requests.append(request)
                    address = request[0]
                    # 23.5 °C，55.1 %RH，或者 400.125 lx。刻意拆帧测试分段接收。
                    pair = (235, 551) if address == 1 else divmod(400125, 65536)
                    frame = struct.pack(">BBBHH", address, 3, 4, *pair)
                    frame += struct.pack("<H", crc16(frame))
                    os.write(master, frame[:3])
                    time.sleep(0.004)
                    os.write(master, frame[3:])

        device = threading.Thread(target=sensors, daemon=True)
        device.start()
        self.addCleanup(device.join, 1)
        self.addCleanup(stop.set)
        monitor_port, mqtt_port = free_port(), free_port()
        while monitor_port == mqtt_port:
            mqtt_port = free_port()

        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.toml"
            config.write_text(f"""[gateway]
interval = 0.5
monitor_port = {monitor_port}
[serial]
device = "{os.ttyname(slave)}"
[mqtt]
port = {mqtt_port}
""")
            broker_config = Path(directory) / "mosquitto.conf"
            broker_config.write_text(f"listener {mqtt_port} 127.0.0.1\nallow_anonymous true\n")

            def start_gateway():
                return subprocess.Popen(
                    [sys.executable, str(ROOT / "gateway.py"), "--config", str(config)],
                    cwd=directory,
                    env={key: value for key, value in os.environ.items()
                         if key not in ("GATEWAY_MQTT_PASSWORD", "NOTIFY_SOCKET")},
                    stdout=subprocess.DEVNULL,
                )

            def status():
                try:
                    with urlopen(f"http://127.0.0.1:{monitor_port}/api/status", timeout=1) as r:
                        return json.load(r)
                except URLError:
                    return {}

            def pending(minimum):
                data = status()
                return data if data.get("pending", 0) >= minimum else None

            gateway = start_gateway()
            try:
                data = until(lambda: pending(3))
                self.assertFalse(data["mqtt_online"])
                self.assertFalse(data["simulated"])
                self.assertEqual(
                    [data["latest"][name] for name in ("temp", "humi", "light")],
                    [23.5, 55.1, 400.125],
                )
                with urlopen(f"http://127.0.0.1:{monitor_port}/", timeout=1) as page:
                    self.assertIn("最近运行日志", page.read().decode())
            finally:
                terminate(gateway)
            self.assertEqual(gateway.returncode, 0)

            with closing(sqlite3.connect(Path(directory) / "gateway.sqlite3")) as db:
                queued_ids = [
                    json.loads(row[0])["sample_id"]
                    for row in db.execute("SELECT payload FROM outbox ORDER BY id")
                ]
            self.assertGreaterEqual(len(queued_ids), 3)
            self.assertEqual(
                [request[:6].hex() for request in requests[:2]],
                ["010300010002", "020300020002"],
            )

            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            received = []
            subscribed = threading.Event()
            client.on_subscribe = lambda *_: subscribed.set()
            client.on_message = lambda _client, _userdata, message: received.append(
                json.loads(message.payload)["sample_id"]
            )
            def start_broker():
                return subprocess.Popen(
                    [BROKER, "-c", str(broker_config)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )

            def flushed():
                data = status()
                return data.get("mqtt_online") and data["pending"] == 0

            broker = start_broker()
            try:
                def connect():
                    try:
                        client.connect("127.0.0.1", mqtt_port)
                        return True
                    except ConnectionRefusedError:
                        return False

                until(connect, 4)
                client.loop_start()
                client.subscribe("edge-gateway/telemetry", qos=1)
                self.assertTrue(subscribed.wait(4))
                gateway = start_gateway()
                try:
                    until(lambda: len(received) >= len(queued_ids))
                    self.assertEqual(received[:len(queued_ids)], queued_ids)
                    until(flushed)

                    # Broker 运行中断线，保持网关进程运行并验证重连补传。
                    terminate(broker)
                    until(lambda: (data := pending(3)) and not data["mqtt_online"])
                    broker = start_broker()
                    until(flushed)
                finally:
                    terminate(gateway)
                self.assertEqual(gateway.returncode, 0)
            finally:
                client.disconnect()
                client.loop_stop()
                terminate(broker)
