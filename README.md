# 工业边缘网关

项目包含 STM32 固件和 Linux C 网关。两版均通过 RS485 轮询温湿度及光照传感器。

| 版本 | 工程入口 |
| --- | --- |
| Linux C 网关 | [原工程目录下的 linux/](工业边缘网关监控最终版本/工业边缘网关监控最终版本/linux/README.md)，包含 C 源码、编译、部署和接线说明 |
| STM32 固件 | [原工程说明](工业边缘网关监控最终版本/README.md)，固件保留原 Keil 工程入口 |

## 编译 Linux 网关

在仓库根目录运行。Debian 或 Ubuntu 需要 C 编译工具和下列开发库。

```bash
sudo apt install build-essential libmodbus-dev libmosquitto-dev libsqlite3-dev libsystemd-dev
cd '工业边缘网关监控最终版本/工业边缘网关监控最终版本'
make -C linux
```

C 主线程负责 Modbus RTU 采集，MQTT 工作线程通过 SQLite 待发队列读取样本并在收到 PUBACK 后出队。本地 HTTP 监控可作为单独的 Python 进程运行。
