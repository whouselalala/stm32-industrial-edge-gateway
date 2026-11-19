#include "gateway.h"

#include <mosquitto.h>
#include <stdio.h>

struct delivery {
    int connack;
    int mid;
    bool acknowledged;
};

static void on_connect(struct mosquitto *client, void *userdata, int code) {
    (void)client;
    struct delivery *state = userdata;
    state->connack = code;
}

static void on_publish(struct mosquitto *client, void *userdata, int mid) {
    (void)client;
    struct delivery *state = userdata;
    if (mid == state->mid) state->acknowledged = true;
}

/* 处理 MQTT socket 和心跳。timeout_ms 是每次等待网络事件的上限。 */
static bool pump(struct mosquitto *client, int timeout_ms) {
    if (stopping()) return false;
    int rc = mosquitto_loop(client, timeout_ms, 1);
    publisher_progress();
    return rc == MOSQ_ERR_SUCCESS;
}

static bool await_connack(struct mosquitto *client, struct delivery *state) {
    int64_t deadline = monotonic_ms() + 5000;
    while (state->connack < 0 && monotonic_ms() < deadline)
        if (!pump(client, 100)) return false;
    return state->connack == 0;
}

/* FIFO 发送。样本先落盘，只有收到对应报文的 PUBACK 才删除这一行。
 * 这样断电或断线后可以重试。确认前后发生中断时 QoS 1 允许重复投递。
 */
static void drain(struct mosquitto *client, struct delivery *state, sqlite3 *db,
                  const char *topic) {
    sqlite3_stmt *next = NULL, *remove = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT id,payload FROM samples ORDER BY id LIMIT 1",
                                -1, &next, NULL);
    if (rc == SQLITE_OK)
        rc = sqlite3_prepare_v2(db, "DELETE FROM samples WHERE id=?", -1, &remove, NULL);
    unsigned sent = 0;
    int64_t next_batch = 0;
    while (rc == SQLITE_OK && !stopping()) {
        if (monotonic_ms() < next_batch) {
            if (!pump(client, 100)) break;
            continue;
        }
        int row = sqlite3_step(next);
        if (row == SQLITE_DONE) {
            sqlite3_reset(next);
            sent = 0;
            if (!pump(client, 100)) break;
            continue;
        }
        if (row != SQLITE_ROW) {
            rc = row;
            break;
        }
        sqlite3_int64 id = sqlite3_column_int64(next, 0);
        const unsigned char *payload = sqlite3_column_text(next, 1);
        int length = sqlite3_column_bytes(next, 1);
        state->acknowledged = false;
        int published = mosquitto_publish(client, &state->mid, topic, length, payload, 1, false);
        sqlite3_reset(next);
        if (published != MOSQ_ERR_SUCCESS) break;

        int64_t deadline = monotonic_ms() + 5000;
        while (!state->acknowledged && monotonic_ms() < deadline)
            if (!pump(client, 100)) break;
        if (!state->acknowledged) break;

        sqlite3_bind_int64(remove, 1, id);
        int deleted = sqlite3_step(remove);
        if (deleted != SQLITE_DONE) {
            rc = deleted;
            break;
        }
        sqlite3_reset(remove);
        if (++sent == REPLAY_BATCH) {
            next_batch = monotonic_ms() + POLL_MS;
            sent = 0;
        }
    }
    if (rc != SQLITE_OK) fprintf(stderr, "MQTT 队列: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(next);
    sqlite3_finalize(remove);
}

void *publish_loop(void *argument) {
    const struct publisher_config *config = argument;
    bool offline_reported = false;
    while (!stopping()) {
        publisher_progress();
        sqlite3 *db = open_store(config->database);
        struct delivery state = {.connack = -1};
        struct mosquitto *client = mosquitto_new(config->client_id, true, &state);
        if (db && client) {
            mosquitto_int_option(client, MOSQ_OPT_PROTOCOL_VERSION, MQTT_PROTOCOL_V311);
            mosquitto_connect_callback_set(client, on_connect);
            mosquitto_publish_callback_set(client, on_publish);
            int rc = mosquitto_connect(client, config->host, config->port, 120);
            publisher_progress();
            if (rc == MOSQ_ERR_SUCCESS && await_connack(client, &state) &&
                mosquitto_subscribe(client, NULL, config->topic, 1) == MOSQ_ERR_SUCCESS &&
                set_mqtt_status(db, true) == 0) {
                log_event(db, "INFO", "MQTT 已连接");
                offline_reported = false;
                drain(client, &state, db, config->topic);
                set_mqtt_status(db, false);
                mosquitto_disconnect(client);
            }
            if (!stopping() && !offline_reported) {
                log_event(db, "WARN", "MQTT 离线，等待重连");
                offline_reported = true;
            }
        }
        if (client) mosquitto_destroy(client);
        sqlite3_close(db);
        publisher_progress();
        pause_ms(RECONNECT_MS);
    }
    return NULL;
}
