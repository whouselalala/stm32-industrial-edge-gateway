#!/usr/bin/env python3
"""Linux 网关入口。主线程轮询 RS485，MQTT 线程独立处理网络发送。"""

import argparse
import fcntl
import json
import logging
import math
import os
import signal
import socket
import sqlite3
import termios
import time
import tomllib
import uuid
from contextlib import closing
from datetime import datetime, timezone
from threading import Event, Thread

import outbox
from modbus import BAUD_RATES, ModbusError, ModbusRTU
from monitor import Monitor, RecentLogs, State
from publisher import Publisher

LOG = logging.getLogger(__name__)


def bounded(value, low, high):
    return type(value) in (int, float) and low <= value <= high


def integer(value, low, high):
    return type(value) is int and low <= value <= high


def load_config(path):
    with open(path, "rb") as file:
        config = tomllib.load(file)
    settings = {
        "gateway": {
            "interval": (2.0, lambda v: bounded(v, 0.1, 3600)),
            "database": ("gateway.sqlite3", lambda v: isinstance(v, str) and bool(v)),
            "queue_limit": (100000, lambda v: integer(v, 1, 1000000)),
            "monitor_port": (8080, lambda v: integer(v, 0, 65535)),
        },
        "serial": {
            "device": ("/dev/ttyUSB0", lambda v: isinstance(v, str) and v.startswith("/dev/")),
            "baud": (9600, lambda v: type(v) is int and v in BAUD_RATES),
            "timeout": (0.5, lambda v: bounded(v, 0.05, 5)),
            "kernel_rs485": (False, lambda v: isinstance(v, bool)),
        },
        "sensors": {
            "th_address": (1, lambda v: integer(v, 1, 247)),
            "th_register": (1, lambda v: integer(v, 0, 65534)),
            "light_address": (2, lambda v: integer(v, 1, 247)),
            "light_register": (2, lambda v: integer(v, 0, 65534)),
            "signed_temperature": (False, lambda v: isinstance(v, bool)),
        },
        "mqtt": {
            "host": ("127.0.0.1", lambda v: isinstance(v, str) and bool(v.strip())),
            "port": (1883, lambda v: integer(v, 1, 65535)),
            "topic": ("edge-gateway/telemetry", lambda v: isinstance(v, str)
                      and 0 < len(v.encode()) <= 65535 and not any(c in v for c in "+#\x00")),
            "client_id": ("linux-edge-gateway", lambda v: isinstance(v, str)
                          and 0 < len(v.encode()) <= 65535 and "\x00" not in v),
            "tls": (False, lambda v: isinstance(v, bool)),
            "ca_file": (None, lambda v: isinstance(v, str) and bool(v)),
            "username": (None, lambda v: isinstance(v, str) and bool(v)),
        },
    }
    for name in config.keys() - settings.keys():
        raise ValueError(f"未知配置段 [{name}]")
    for section, fields in settings.items():
        values = config.setdefault(section, {})
        if not isinstance(values, dict):
            raise ValueError(f"[{section}] 必须是 TOML 表")
        for name in values.keys() - fields.keys():
            raise ValueError(f"未知设置 {section}.{name}")
        for name, (default, check) in fields.items():
            if name in values and not check(values[name]):
                raise ValueError(f"配置值无效 {section}.{name}")
            values.setdefault(name, default)
    return config


class Sensors:
    def __init__(self, config, simulate):
        self.mapping = config["sensors"]
        self.serial = ModbusRTU(**config["serial"])
        self.simulate = simulate
        self.start = time.monotonic()
        self.errors = {}

    def _report(self, device, reason):
        previous = self.errors.get(device)
        if reason != previous:
            if reason is None:
                LOG.info("%s 已恢复", device)
            else:
                LOG.warning("%s 读取失败：%s", device, reason)
        self.errors[device] = reason

    def _pair(self, name, address, register):
        try:
            # USB 转换器断开时关闭句柄，下一次采集重新打开设备路径。
            if self.serial.fd is None:
                self.serial.open()
            values = self.serial.read_pair(address, register)
        except ModbusError as error:
            self._report(name, str(error))
            return None
        except (OSError, termios.error) as error:
            self.serial.close()
            self._report(name, str(error))
            return None
        self._report(name, None)
        return values

    def sample(self):
        if self.simulate:
            wave = math.sin((time.monotonic() - self.start) / 12)
            return round(24 + 2 * wave, 1), round(52 + 5 * wave, 1), round(400 + 60 * wave, 3)

        mapping = self.mapping
        th = self._pair("温湿度", mapping["th_address"], mapping["th_register"])
        lux = self._pair("光照", mapping["light_address"], mapping["light_register"])
        temp = humidity = light = None
        if th is not None:
            raw, rh = th
            if mapping["signed_temperature"] and raw >= 0x8000:
                raw -= 0x10000
            temp, humidity = raw / 10.0, rh / 10.0
        if lux is not None:
            # 光照值的高字在前，两个寄存器组成一个无符号 32 位量。
            light = ((lux[0] << 16) | lux[1]) / 1000.0
        return temp, humidity, light


def notify_systemd(message):
    address = os.environ.get("NOTIFY_SOCKET")
    if address:
        if address.startswith("@"):
            address = "\0" + address[1:]
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.settimeout(0.25)
            sock.sendto(message.encode(), address)


def collect(config, simulate, database, state, publisher, stop, sensors):
    rejected = 0
    full = False
    interval = config["gateway"]["interval"]
    limit = config["gateway"]["queue_limit"]
    topic = config["mqtt"]["topic"]
    while not stop.is_set():
        start = time.monotonic()
        temp, humidity, light = sensors.sample()
        sample = {
            "sample_id": uuid.uuid4().hex,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "temp": temp,
            "humi": humidity,
            "light": light,
            "simulated": simulate,
        }
        if any(value is not None for value in (temp, humidity, light)):
            saved = outbox.enqueue(database, topic, json.dumps(sample), limit)
            if not saved:
                rejected += 1
                if not full:
                    LOG.warning("离线队列达到 %d 条，开始拒绝新样本", limit)
            elif full:
                LOG.info("离线队列恢复入队")
            full = not saved
        state.update(latest=sample, queue_rejected_since_start=rejected)

        # 长轮询间隔内也发送看门狗心跳，同时要求 MQTT 线程有进展。
        while not stop.is_set():
            if time.monotonic() - publisher.progress < 20:
                notify_systemd("WATCHDOG=1")
            remaining = start + interval - time.monotonic()
            if remaining <= 0:
                break
            stop.wait(min(remaining, 1.0))


def run(config, simulate):
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    settings = config["gateway"]
    database = settings["database"]
    state = State(simulate)
    logs = RecentLogs()
    logs.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(logs)
    sensors = Sensors(config, simulate)

    # 锁定数据库文件，避免第二个网关进程同时轮询串口、发送同一队列。
    with open(database, "ab") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("已有网关进程在使用此数据库") from None
        outbox.initialize(database)
        publisher = Publisher(config["mqtt"], database, stop, state)
        monitor = monitor_thread = None
        try:
            if settings["monitor_port"]:
                monitor = Monitor(settings["monitor_port"], database, state, logs)
                monitor_thread = Thread(
                    target=monitor.serve_until, args=(stop,), name="monitor", daemon=True
                )
                monitor_thread.start()
            publisher.start()
            LOG.info("Linux 网关已启动，采集来源：%s", "模拟" if simulate else "RS485")
            notify_systemd("READY=1")
            with closing(outbox.connect(database)) as db:
                collect(config, simulate, db, state, publisher, stop, sensors)
        finally:
            stop.set()
            sensors.serial.close()
            if monitor_thread is not None and monitor_thread.ident is not None:
                monitor_thread.join(timeout=3.0)
            if monitor is not None:
                monitor.server_close()
            if publisher.ident is not None:
                publisher.join(timeout=5.0)
    if publisher.error is not None:
        raise RuntimeError("MQTT 发送线程已停止") from publisher.error


def main():
    parser = argparse.ArgumentParser(description="Linux 工业边缘网关")
    parser.add_argument("--config", default="config.toml", help="TOML 配置文件路径")
    parser.add_argument("--simulate", action="store_true", help="使用模拟数据，跳过串口")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(load_config(args.config), args.simulate)
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        LOG.error("网关退出：%s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
