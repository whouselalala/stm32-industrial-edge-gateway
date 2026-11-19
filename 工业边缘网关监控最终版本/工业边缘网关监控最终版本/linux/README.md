# 工业物联网边缘网关 V2.0 · Linux 主机

基于 [zgd-dev/-V2.0](https://github.com/zgd-dev/-V2.0/tree/40b4460e1f576e0f8223338f06698bc6bdd1bf90) 的 STM32 主机移植。Linux 设备通过自动收发的 USB–RS485 转接器连接原有温湿度和光照从站，再通过 Linux 网络接入巴法云。上游工程提供的是网关主机固件，温湿度和光照设备是外接从站。

## 从站接线与协议

把两台从站接回同一条 RS485 总线，A 接 A、B 接 B。串口参数为 **9600、8N1**，两台设备都使用功能码 **0x03** 读保持寄存器。

| 设备 | 从站地址 | 起始寄存器（零基） | 数量 | 解析 |
|---|---:|---:|---:|---|
| 温湿度 | 1 | 0x0001 | 2 | 第一字温度、第二字湿度，按原主机的无符号数值各除以 10 |
| 光照 | 2 | 0x0002 | 2 | 高字在前，拼成 32 位无符号数后整除 1000，单位 lx |

每台设备读完留出 30 ms 间隔，一轮结束后等待 800 ms。从站超时或 CRC 错误时保留上次读数，页面同时显示从站离线。两台都失败时重开串口。

## MQTT 兼容

默认服务器 `bemfa.com:9501`，发布和订阅主题为 `stm32`，发布使用 QoS 1。把你自己的巴法云私钥作为 `GATEWAY_MQTT_CLIENT_ID` 配置，并在巴法云控制台创建对应主题。见 [巴法云 MQTT 接入文档](https://cloud.bemfa.com/docs/src/mqtt.html)。

| 原主机路径 | 保留的消息示例 |
|---|---|
| 在线直报 | `{{"temp":23.5,"humi":56.7,"light":123}}` |
| 离线缓存补传 | `{"temperature":23.5,"shidu":56.7,"light":123}` |

在线样例的双花括号来自原始 `model/modbus.c`，属于原有的 MQTT 文本格式。离线报文对应原始 `code/Taskhandel.c` 的 Flash 缓存路径。

样本由 C 采集线程写入 SQLite，MQTT 工作线程按顺序发送。重连后每批最多补传 5 条，收到 PUBACK 后才删除对应记录，后续批次会继续发送积压。断电发生在确认与删除之间时，同一条记录可能重复投递。缓存和运行日志各自循环淘汰最早记录，条数上限按原来的 4 MiB 分区折算。SQLite 数据库文件大小取决于页和 WAL 开销。

## 编译与本地运行

适用于装有 systemd 的 Debian / Ubuntu。以下命令在本 `linux/` 目录运行。

```sh
sudo apt install build-essential libmodbus-dev libmosquitto-dev libsqlite3-dev libsystemd-dev python3
make
ls /dev/serial/by-id/
read -rsp '巴法云私钥: ' GATEWAY_MQTT_CLIENT_ID; printf '\n'
export GATEWAY_MQTT_CLIENT_ID
./edge-gateway /dev/ttyUSB0
```

如果普通用户无法打开串口，用 `sudo usermod -aG dialout "$USER"` 加入串口组后重新登录。另开一个终端启动监控页面。

```sh
python3 ui/server.py gateway.sqlite3
```

同一台 Linux 设备上的浏览器访问 [http://127.0.0.1:8080](http://127.0.0.1:8080)。页面包含实时读数、从站和 MQTT 状态、运行时间、队列积压及倒序日志。10 秒空闲时页面变黑，可触摸、按键或由新采样唤醒。原 SPI 屏、触摸驱动和背光控制属于 STM32 主机外设。Linux 版用浏览器承载这些界面功能，变黑仅发生在页面层。

| 参数 | 含义 | 默认值 |
|---|---|---|
| 第一个位置参数 | Linux 串口设备路径 | 必填 |
| `GATEWAY_DATABASE` | SQLite 文件位置 | `gateway.sqlite3` |
| `GATEWAY_MQTT_HOST` | MQTT Broker 地址 | `bemfa.com` |
| `GATEWAY_MQTT_PORT` | MQTT TCP 端口 | `9501` |
| `GATEWAY_MQTT_TOPIC` | MQTT 主题 | `stm32` |
| `GATEWAY_MQTT_CLIENT_ID` | 巴法云私钥或本地 Broker 客户端 ID | 空时由库生成 |

也可显式传入参数，例如 `./edge-gateway /dev/ttyUSB0 gateway.sqlite3 127.0.0.1 1883 stm32`。客户端 ID 仍从环境读取。

## systemd 部署

以下命令同样在本 `linux/` 目录执行。先运行 `make`。

```sh
getent passwd edge-gateway >/dev/null || sudo useradd --system --user-group --no-create-home edge-gateway
sudo install -m 0755 edge-gateway /usr/local/bin/edge-gateway
sudo install -d /usr/local/share/edge-gateway
sudo install -m 0644 ui/server.py ui/index.html /usr/local/share/edge-gateway/
sudo install -m 0644 deploy/*.service /etc/systemd/system/
sudo install -m 0600 deploy/edge-gateway.conf.example /etc/edge-gateway.conf
sudoedit /etc/edge-gateway.conf
sudo systemctl daemon-reload
sudo systemctl enable --now edge-gateway.service edge-gateway-ui.service
journalctl -u edge-gateway.service -f
```

网关通过 systemd 的 20 s 服务看门狗检测采集或网络线程卡死，超时会重启网关进程。数据库保存在 `/var/lib/edge-gateway`。查看两个线程的实现从 `main.c` 和 `publisher.c` 开始，`store.c` 负责样本、日志和界面状态。


## 从旧 Python 版切换

C 版使用 Linux 工程的巴法云格式和 `deploy/edge-gateway.conf.example`。旧版 `config.toml`、`--simulate` 和 MQTT TLS/用户名配置属于旧版入口。没有传感器时，使用测试章节的伪终端测试运行采集链路。旧 SQLite 数据库的待发记录在 `outbox` 表中，C 版读取的是 `samples` 表。切换前可由旧服务发完积压，再停用旧的 `linux-edge-gateway.service`，用新的数据库路径启动 C 版。

## 测试

```sh
sudo apt install mosquitto mosquitto-clients
make
python3 tests/e2e.py
```

测试使用 Linux 伪终端模拟两个从站，用本地 Mosquitto 验证在线报文、断线缓存、进程重启后的补传和重新连接。还会检查监控 API 及看门狗通知。
