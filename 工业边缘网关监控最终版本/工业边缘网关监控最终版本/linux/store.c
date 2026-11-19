#include "gateway.h"

#include <stdio.h>
#include <time.h>

/* 原主机分别为日志和缓存预留 4 MiB。以原固定记录的大小折算条数，
 * 日志每条 76 字节，缓存每条 128 字节。SQLite 文件含索引和 WAL，会额外占空间。
 */
#define MAX_LOGS "55188"
#define MAX_OUTBOX "32768"

static int execute(sqlite3 *db, const char *sql) {
    if (sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK) return 0;
    fprintf(stderr, "SQLite: %s\n", sqlite3_errmsg(db));
    return -1;
}

sqlite3 *open_store(const char *path) {
    sqlite3 *db = NULL;
    if (sqlite3_open(path, &db) == SQLITE_OK) {
        sqlite3_busy_timeout(db, 2000);
        if (execute(db, "PRAGMA journal_mode=WAL;"
                    "CREATE TABLE IF NOT EXISTS samples("
                    "id INTEGER PRIMARY KEY, payload TEXT NOT NULL);"
                    "CREATE TABLE IF NOT EXISTS run_log("
                    "id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, "
                    "level TEXT NOT NULL, message TEXT NOT NULL);"
                    "CREATE TABLE IF NOT EXISTS state("
                    "id INTEGER PRIMARY KEY CHECK(id=1), mqtt INTEGER NOT NULL,"
                    "started_at INTEGER, updated_at INTEGER, temperature REAL, humidity REAL,"
                    "light INTEGER, temperature_online INTEGER, light_online INTEGER);"
                    "INSERT OR IGNORE INTO state(id,mqtt) VALUES(1,0);") == 0)
            return db;
    }
    fprintf(stderr, "打开数据库失败: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return NULL;
}

int start_session(sqlite3 *db) {
    return execute(db, "UPDATE state SET mqtt=0,started_at=CAST(strftime('%s','now') AS INTEGER),updated_at=NULL,"
                   "temperature=NULL,humidity=NULL,light=NULL,temperature_online=0,light_online=0");
}

int set_mqtt_status(sqlite3 *db, bool online) {
    return execute(db, online ? "UPDATE state SET mqtt=1" : "UPDATE state SET mqtt=0");
}

int log_event(sqlite3 *db, const char *level, const char *message) {
    fprintf(stderr, "%s %s\n", level, message);
    char *sql = sqlite3_mprintf(
        "BEGIN IMMEDIATE;"
        "INSERT INTO run_log(ts,level,message) VALUES(%lld,%Q,%Q);"
        "DELETE FROM run_log WHERE id <= (SELECT max(id)-" MAX_LOGS " FROM run_log);"
        "COMMIT;", (long long)time(NULL), level, message);
    int result = sql ? execute(db, sql) : -1;
    sqlite3_free(sql);
    if (result < 0) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    return result;
}

int store_sample(sqlite3 *db, const struct readings *sample) {
    /* 忠实沿用原温湿度解析（无符号 / 10）和光照解析（高字在前，整数除以 1000）。 */
    double temperature = sample->temperature / 10.0;
    double humidity = sample->humidity / 10.0;
    unsigned light = sample->light / 1000;
    char online[112], offline[112];
    /* 在线报文和离线报文分别对应 MQTT_SendAllDev_NoWait 与 FlashCache_Write 路径。 */
    snprintf(online, sizeof online, "{{\"temp\":%.1f,\"humi\":%.1f,\"light\":%u}}",
             temperature, humidity, light);
    snprintf(offline, sizeof offline, "{\"temperature\":%.1f,\"shidu\":%.1f,\"light\":%u}",
             temperature, humidity, light);

    if (execute(db, "BEGIN IMMEDIATE") < 0) return -1;
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "UPDATE state SET updated_at=?,temperature=?,humidity=?,light=?,"
        "temperature_online=?,light_online=? WHERE id=1", -1, &statement, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_int64(statement, 1, time(NULL));
        if (sample->temperature_valid) {
            sqlite3_bind_double(statement, 2, temperature);
            sqlite3_bind_double(statement, 3, humidity);
        }
        if (sample->light_valid) sqlite3_bind_int(statement, 4, (int)light);
        sqlite3_bind_int(statement, 5, sample->temperature_online);
        sqlite3_bind_int(statement, 6, sample->light_online);
        rc = sqlite3_step(statement);
    }
    sqlite3_finalize(statement);
    if (rc == SQLITE_DONE) {
        char *sql = sqlite3_mprintf(
            "INSERT INTO samples(payload) SELECT CASE mqtt WHEN 1 THEN %Q ELSE %Q END FROM state;"
            "DELETE FROM samples WHERE id <= (SELECT max(id)-" MAX_OUTBOX " FROM samples);"
            "COMMIT;", online, offline);
        int result = sql ? execute(db, sql) : -1;
        sqlite3_free(sql);
        if (result == 0) return 0;
    } else {
        fprintf(stderr, "保存样本失败: %s\n", sqlite3_errmsg(db));
    }
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    return -1;
}
