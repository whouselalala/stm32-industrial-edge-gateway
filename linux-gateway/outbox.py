"""SQLite 持久化发送队列。采集与发送线程分别持有数据库连接。"""

import sqlite3
from contextlib import closing


def connect(path):
    db = sqlite3.connect(path, timeout=5, isolation_level=None)
    try:
        # SQLite 默认的 FULL 同步显式写出，确保网络发送前已经提交。
        db.execute("PRAGMA synchronous=FULL")
    except sqlite3.Error:
        db.close()
        raise
    return db


def initialize(path):
    with closing(connect(path)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("""CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL
        )""")


def enqueue(db, topic, payload, limit):
    # 单条语句在同一写事务内判断上限并插入，满时保留待补传的旧记录。
    cursor = db.execute(
        """INSERT INTO outbox (topic, payload)
           SELECT ?, ? WHERE (SELECT COUNT(*) FROM outbox) < ?""",
        (topic, payload, limit),
    )
    return cursor.rowcount == 1


def oldest(db):
    return db.execute(
        "SELECT id, topic, payload FROM outbox ORDER BY id LIMIT 1"
    ).fetchone()


def acknowledge(db, row_id):
    db.execute("DELETE FROM outbox WHERE id = ?", (row_id,))


def pending(db):
    return db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
