# Linux 工业边缘网关

在 Linux 工控机、树莓派或其他 Linux 主机上运行。网关通过 RS485 轮询温湿度和光照传感器，将样本写入 SQLite，再通过 MQTT 上报。浏览器可查看实时读数、网络状态、积压条数和最近日志。

| 功能 | Linux 实现 |
| --- | --- |
| 环境数据采集 | Linux `termios` 串口，Modbus RTU 主站轮询两个从站 |
| 云端上报 | MQTT 3.1.1，QoS 1，支持 TLS、心跳和自动重连 |
| 断网续传 | SQLite WAL 持久化队列，先入库、后上报，收到 PUBACK 后删除 |
| 本地监控 | 本机 HTTP 页面与 JSON 状态接口，运行日志同时写入标准错误 |
| 服务恢复 | systemd 管理进程，异常退出重启，采集和 MQTT 线程停止推进时触发服务看门狗 |

## 快速运行

需要 Linux 和 Python 3.11 或更新版本。Ubuntu 24.04 上可用以下命令。若设备上已有 MQTT broker，直接在配置中填写其连接信息。

```bash
sudo apt install python3-venv mosquitto mosquitto-clients
cd linux-gateway
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp config.example.toml config.toml
.venv/bin/python gateway.py --config config.toml --simulate
```

用浏览器打开 [本地监控页](http://127.0.0.1:8080/)，或在另一个终端订阅。若没有运行 broker，模拟采集仍会继续，监控页面会显示离线和待确认条数。

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -q 1 -t edge-gateway/telemetry -v
```

网关在远程 Linux 主机运行时，可以用 `ssh -L 8080:127.0.0.1:8080 user@linux-host` 将监控页转发到本机。

## 接入原项目的传感器

1. 用具备自动收发方向控制的 USB–RS485 转换器（或 Linux 驱动支持 RS485 模式的板载串口）连接两个传感器，按传感器说明书供电并核对 A/B 线。
2. 在 `config.toml` 的 `[serial]` 中填实际设备路径。USB 转换器优先使用 `/dev/serial/by-id/...`。当前用户须有串口读写权限。Ubuntu 通常使用 `dialout` 组，加入组后重新登录。
3. 核对下表的从站地址和保持寄存器。默认是 **9600、8N1**。配置里的寄存器地址是 Modbus 请求帧中的 **零基地址**。
4. 停止模拟进程并去掉 `--simulate` 启动实机。模拟和实机使用不同数据库路径或 MQTT 主题，可避免混合两类数据。

| 从站 | 默认地址 | 功能码 | 起始寄存器 | 数据换算 |
| --- | --- | --- | --- | --- |
| 温湿度 | 1 | `0x03` | `0x0001` | 第 1 个寄存器 ÷ 10 得 °C，第 2 个 ÷ 10 得 %RH |
| 光照 | 2 | `0x03` | `0x0002` | 两个寄存器按高字、低字拼成 uint32，除以 1000 得 lx |

温度默认沿用原固件的无符号解析。传感器手册规定有符号温度时，设置 `signed_temperature = true`。板载串口使用驱动控制收发方向时，可设置 `kernel_rs485 = true`，代码要求发送期间 RTS 为高，接收期间为低。USB 自动收发转换器保持 `false`。

单个从站失败时，对应读数为 `null`，另一从站的有效值仍会上报。两个从站都失败时，本轮只更新本地状态。串口重新可用后会继续采集。

## MQTT 上报与断网续传

在 `[mqtt]` 设置 broker 地址、端口、Topic 和唯一的 `client_id`。需要认证时填写 `username`，密码通过 `GATEWAY_MQTT_PASSWORD` 环境变量提供。TLS 默认使用系统 CA，私有 CA 可配置 `ca_file`。旧固件使用的云端服务需要按实际账号配置，并确认支持 MQTT 3.1.1 和 QoS 1。

```json
{
  "sample_id": "128cb5a1bc99420e8df9927db26489dd8",
  "timestamp": "2026-11-19T11:20:30.125+00:00",
  "temp": 23.5,
  "humi": 55.1,
  "light": 400.125,
  "simulated": false
}
```

- 采集线程把每条有效样本和对应 Topic 提交到 SQLite。网络线程按入队顺序发送，重连或进程重启后从队首继续。
- QoS 1 可能重复交付。例如 broker 已收到消息，但网关在清除记录前重启。下游可以用 `sample_id` 去重。
- 队列默认保留最多 100,000 条未确认记录。满时保留已入队记录，拒绝新样本并在监控页计数。配置中的数据库相对路径以运行目录为基准。

## 部署为 systemd 服务

以下命令在 `linux-gateway/` 下执行。服务示例适用于有 `dialout` 组的 Linux 发行版，按目标主机的串口设备组调整 `SupplementaryGroups`。

```bash
sudo install -d /opt/linux-edge-gateway /etc/linux-edge-gateway
sudo install -m 0644 *.py monitor.html requirements.txt /opt/linux-edge-gateway/
sudo python3 -m venv /opt/linux-edge-gateway/.venv
sudo /opt/linux-edge-gateway/.venv/bin/pip install -r /opt/linux-edge-gateway/requirements.txt
sudo test -f /etc/linux-edge-gateway/config.toml ||
  sudo install -m 0644 config.example.toml /etc/linux-edge-gateway/config.toml
sudoedit /etc/linux-edge-gateway/config.toml
sudo install -m 0644 systemd/linux-edge-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linux-edge-gateway.service
journalctl -u linux-edge-gateway.service -f
```

保留默认相对数据库路径时，数据库位于 `/var/lib/linux-edge-gateway/gateway.sqlite3`。若 MQTT 需密码，在权限为 `0600` 的 `/etc/linux-edge-gateway/mqtt.env` 中设置 `GATEWAY_MQTT_PASSWORD`。服务看门狗恢复网关进程，日志可通过 journald 查询。

## 源码和验证

| 文件 | 职责 |
| --- | --- |
| [gateway.py](linux-gateway/gateway.py) | 加载配置、采集与换算、管理进程生命周期 |
| [modbus.py](linux-gateway/modbus.py) | 配置 Linux 串口、组帧、超时和 CRC 校验 |
| [outbox.py](linux-gateway/outbox.py) | SQLite 持久化发送队列 |
| [publisher.py](linux-gateway/publisher.py) | MQTT 心跳、重连与 QoS 1 确认 |
| [monitor.py](linux-gateway/monitor.py) | HTTP 状态接口、最近 100 条日志 |
| [config.example.toml](linux-gateway/config.example.toml) | 参数和单位说明 |

在 `linux-gateway/` 中执行 `.venv/bin/python -m unittest discover -s tests -v`。安装 Mosquitto 后会额外运行虚拟串口到真实 broker 的端到端测试，覆盖寄存器解码、监控页、断网缓存、进程重启与积压补传。

原 [STM32 固件及说明](工业边缘网关监控最终版本/README.md) 可用于对照移植。原板级 LCD、背光与触摸功能仍在固件中。Linux 的显示入口是浏览器，故障恢复由 systemd 管理。
