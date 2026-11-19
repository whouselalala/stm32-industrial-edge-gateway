"""本机只读监控，展示最新采样、MQTT 状态和最近运行日志。"""

import json
import logging
from collections import deque
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Lock

import outbox


class State:
    def __init__(self, simulated):
        self.lock = Lock()
        self.values = {
            "simulated": simulated,
            "mqtt_online": False,
            "latest": None,
            "queue_rejected_since_start": 0,
        }

    def update(self, **values):
        with self.lock:
            self.values.update(values)

    def snapshot(self):
        with self.lock:
            return self.values.copy()


class RecentLogs(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = deque(maxlen=100)

    def emit(self, record):
        # logging.Handler.handle 在调用 emit 时已持有同一把锁。
        self.records.appendleft(self.format(record))

    def snapshot(self):
        with self.lock:
            return list(self.records)


class Monitor(HTTPServer):
    timeout = 0.25

    def __init__(self, port, database, state, logs):
        self.database = database
        self.state = state
        self.logs = logs
        self.page = Path(__file__).with_name("monitor.html").read_bytes()
        super().__init__(("127.0.0.1", port), Handler)

    def get_request(self):
        connection, peer = super().get_request()
        connection.settimeout(2.0)
        return connection, peer

    def serve_until(self, stop):
        # 此连接只由 HTTP 线程使用。每次查询都读取 SQLite 的当前提交版本。
        with closing(outbox.connect(self.database)) as db:
            self.db = db
            while not stop.is_set():
                self.handle_request()


class Handler(BaseHTTPRequestHandler):
    def reply(self, data, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/":
            self.reply(self.server.page, "text/html")
        elif self.path == "/api/status":
            state = self.server.state.snapshot()
            state["pending"] = outbox.pending(self.server.db)
            state["logs"] = self.server.logs.snapshot()
            self.reply(json.dumps(state, ensure_ascii=False).encode(), "application/json")
        else:
            self.send_error(404)

    def log_message(self, _format, *_args):
        # 轮询监控接口不写入业务日志。
        pass
