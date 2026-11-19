#define _POSIX_C_SOURCE 200809L
#include "gateway.h"

#include <errno.h>
#include <modbus/modbus.h>
#include <mosquitto.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <systemd/sd-daemon.h>
#include <time.h>

static volatile sig_atomic_t interrupted;
static atomic_bool shutdown_requested;
static atomic_llong mqtt_progress;

/* 使用单调时钟计算等待时间，系统墙上时钟调整不会改变超时。 */
static struct timespec current_time(void) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return value;
}

int64_t monotonic_ms(void) {
    struct timespec value = current_time();
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

static void on_signal(int signal_number) {
    (void)signal_number;
    interrupted = 1;
}

bool stopping(void) {
    return atomic_load(&shutdown_requested);
}

void pause_ms(unsigned milliseconds) {
    int64_t deadline = monotonic_ms() + milliseconds;
    while (!stopping()) {
        int64_t remaining = deadline - monotonic_ms();
        if (remaining <= 0) return;
        long step = remaining > 100 ? 100 : (long)remaining;
        struct timespec delay = {0, step * 1000000L};
        nanosleep(&delay, NULL);
    }
}

void publisher_progress(void) {
    atomic_store(&mqtt_progress, monotonic_ms());
}

bool publisher_alive(void) {
    return monotonic_ms() - atomic_load(&mqtt_progress) < 8000;
}

static const char *setting(const char *name, const char *fallback) {
    const char *value = getenv(name);
    return value && *value ? value : fallback;
}

/* 读取两个保持寄存器。slave 是从站设备地址，start 是设备内零基寄存器地址。 */
static bool read_registers(modbus_t *bus, int slave, int start, uint16_t words[2]) {
    if (modbus_set_slave(bus, slave) == 0 &&
        modbus_read_registers(bus, start, 2, words) == 2)
        return true;
    modbus_flush(bus);
    return false;
}

static int report_sensor(sqlite3 *db, const char *name, bool old_status, bool new_status) {
    if (old_status == new_status) return 0;
    char message[96];
    snprintf(message, sizeof message, "%s从站%s", name, new_status ? "在线" : "离线");
    return log_event(db, new_status ? "INFO" : "WARN", message);
}

/* 串口只由采集线程访问。出现异常时保留上次读数，同时更新从站在线状态。 */
static int collect(sqlite3 *db, modbus_t *bus, const char *serial) {
    struct readings sample = {0};
    bool connected = false;
    bool serial_error_reported = false;
    int result = 0;
    while (!interrupted) {
        /* 采集循环和 MQTT 线程都在推进时才通知 systemd。 */
        if (publisher_alive()) sd_notify(0, "WATCHDOG=1");
        if (!connected) {
            if (modbus_connect(bus) < 0) {
                if (!serial_error_reported) {
                    char message[256];
                    snprintf(message, sizeof message, "打开串口 %s 失败: %s",
                             serial, modbus_strerror(errno));
                    log_event(db, "WARN", message);
                    serial_error_reported = true;
                }
                pause_ms(RECONNECT_MS);
                continue;
            }
            connected = true;
            if (serial_error_reported) log_event(db, "INFO", "串口已恢复");
            serial_error_reported = false;
        }

        bool temperature_was_online = sample.temperature_online;
        bool light_was_online = sample.light_online;
        uint16_t words[2];
        if ((sample.temperature_online = read_registers(bus, 1, 0x0001, words))) {
            sample.temperature = words[0];
            sample.humidity = words[1];
            sample.temperature_valid = true;
        }
        pause_ms(INTER_DEVICE_MS);
        if (interrupted) break;
        if ((sample.light_online = read_registers(bus, 2, 0x0002, words))) {
            /* 从站先返回高 16 位，拼接后由存储层换算成 lx。 */
            sample.light = ((uint32_t)words[0] << 16) | words[1];
            sample.light_valid = true;
        }
        pause_ms(INTER_DEVICE_MS);
        if (report_sensor(db, "温湿度", temperature_was_online, sample.temperature_online) < 0 ||
            report_sensor(db, "光照", light_was_online, sample.light_online) < 0 ||
            store_sample(db, &sample) < 0) {
            result = 1;
            break;
        }
        if (!sample.temperature_online && !sample.light_online) {
            modbus_close(bus);
            connected = false;
        }
        pause_ms(POLL_MS);
    }
    if (connected) modbus_close(bus);
    return result;
}

int main(int argc, char **argv) {
    if (argc != 2 && argc != 6) {
        fprintf(stderr, "用法: %s SERIAL [DATABASE MQTT_HOST MQTT_PORT TOPIC]\n", argv[0]);
        return 2;
    }
    const char *database = argc == 6 ? argv[2] : setting("GATEWAY_DATABASE", "gateway.sqlite3");
    const char *host = argc == 6 ? argv[3] : setting("GATEWAY_MQTT_HOST", "bemfa.com");
    const char *port_text = argc == 6 ? argv[4] : setting("GATEWAY_MQTT_PORT", "9501");
    const char *topic = argc == 6 ? argv[5] : setting("GATEWAY_MQTT_TOPIC", "stm32");
    const char *client_id = setting("GATEWAY_MQTT_CLIENT_ID", NULL);
    char *end;
    errno = 0;
    long port = strtol(port_text, &end, 10);
    if (errno || *end || port < 1 || port > 65535 || !*host ||
        mosquitto_pub_topic_check(topic) != MOSQ_ERR_SUCCESS) {
        fputs("MQTT 主机、端口或主题无效\n", stderr);
        return 2;
    }
    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) return 1;

    /* 由主线程处理终止信号，子线程继承阻塞掩码，避免跨线程异步写入。 */
    struct sigaction action = {.sa_handler = on_signal};
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, NULL);

    int result = 1;
    /* 主线程采集，发送线程自己打开同一数据库，两者用队列表交接。 */
    sqlite3 *db = open_store(database);
    modbus_t *bus = modbus_new_rtu(argv[1], 9600, 'N', 8, 1);
    if (db && bus && start_session(db) == 0 &&
        log_event(db, "INFO", "网关启动") == 0) {
        /* 原主机的首帧超时 800 ms，字节空闲超时 50 ms。 */
        modbus_set_response_timeout(bus, 0, 800000);
        modbus_set_byte_timeout(bus, 0, 50000);
        struct publisher_config config = {database, host, topic, client_id, (int)port};
        pthread_t publisher;
        publisher_progress();
        if (pthread_create(&publisher, NULL, publish_loop, &config) == 0) {
            pthread_sigmask(SIG_UNBLOCK, &signals, NULL);
            sd_notify(0, "READY=1");
            result = collect(db, bus, argv[1]);
            atomic_store(&shutdown_requested, true);
            pthread_join(publisher, NULL);
            log_event(db, "INFO", "网关停止");
        }
    }
    if (bus) modbus_free(bus);
    sqlite3_close(db);
    mosquitto_lib_cleanup();
    return result;
}
