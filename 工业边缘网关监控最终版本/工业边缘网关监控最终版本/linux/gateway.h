#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <sqlite3.h>

enum { POLL_MS = 800, INTER_DEVICE_MS = 30, RECONNECT_MS = 2000, REPLAY_BATCH = 5 };

/* bus 保留原项目最后一次有效的读数。valid 用于本地界面区分初始值与有效测量。 */
struct readings {
    uint16_t temperature;
    uint16_t humidity;
    uint32_t light;
    bool temperature_valid, light_valid;
    bool temperature_online, light_online;
};

struct publisher_config {
    const char *database, *host, *topic, *client_id;
    int port;
};

bool stopping(void);
void pause_ms(unsigned milliseconds);
int64_t monotonic_ms(void);

/* 每次网络循环完成后更新进度。主线程用它判定 MQTT 线程是否仍在运行。 */
void publisher_progress(void);
bool publisher_alive(void);

sqlite3 *open_store(const char *path);
/* 每次轮询在同一个事务中更新界面状态和待发报文。 */
int store_sample(sqlite3 *db, const struct readings *sample);
int start_session(sqlite3 *db);
int set_mqtt_status(sqlite3 *db, bool online);
int log_event(sqlite3 *db, const char *level, const char *message);

void *publish_loop(void *argument);
