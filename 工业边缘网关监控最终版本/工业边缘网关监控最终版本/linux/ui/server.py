"""本地监控界面。只读访问网关数据库，无需第三方 Python 库。"""

import argparse
import json
import sqlite3
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

PAGE = Path(__file__).with_name("index.html").read_bytes()


class MonitorHandler(BaseHTTPRequestHandler):
    def send_bytes(self, status, content_type, payload):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def log_request(self, code="-", size="-"):
        # 页面每秒轮询 API，访问日志只记录异常请求。
        if str(code).startswith(("4", "5")) and self.path != "/api/status":
            super().log_request(code, size)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/":
            self.send_bytes(200, "text/html; charset=utf-8", PAGE)
            return
        if path != "/api/status":
            self.send_error(404)
            return
        try:
            with closing(sqlite3.connect(self.server.database_uri, uri=True, timeout=1)) as db:
                db.row_factory = sqlite3.Row
                state = db.execute("SELECT * FROM state WHERE id=1").fetchone()
                logs = db.execute(
                    "SELECT ts,level,message FROM run_log ORDER BY id DESC LIMIT 100"
                ).fetchall()
                pending = db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
            body = {"state": dict(state), "pending": pending, "logs": [dict(row) for row in logs]}
        except sqlite3.Error:
            self.send_bytes(503, "application/json", b'{"error":"gateway database unavailable"}')
            return
        self.send_bytes(200, "application/json; charset=utf-8",
                        json.dumps(body, ensure_ascii=False).encode())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path, help="与 C 网关使用同一个 SQLite 文件")
    parser.add_argument("--port", type=int, default=8080, help="本地 HTTP 端口，默认 8080")
    args = parser.parse_args()
    with ThreadingHTTPServer(("127.0.0.1", args.port), MonitorHandler) as server:
        server.database_uri = args.database.resolve().as_uri() + "?mode=ro"
        server.serve_forever()


if __name__ == "__main__":
    main()
