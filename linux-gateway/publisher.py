"""通过 Linux 网络发布 MQTT 3.1.1 遥测。"""

import logging
import os
import ssl
import time
from contextlib import closing
from threading import Thread

import paho.mqtt.client as mqtt

import outbox

LOG = logging.getLogger(__name__)
ACK_TIMEOUT = 5.0


class BrokerError(Exception):
    """MQTT 连接失败、协议错误或确认超时。"""


class Publisher(Thread):
    def __init__(self, config, database, stop, state):
        super().__init__(name="mqtt", daemon=True)
        self.config = config
        self.database = database
        self.stop = stop
        self.state = state
        self.progress = time.monotonic()
        self.error = None
        self.tls = None
        if config["tls"]:
            self.tls = ssl.create_default_context(cafile=config["ca_file"])
        self.password = os.environ.get("GATEWAY_MQTT_PASSWORD")
        if self.password is not None and not config["username"]:
            raise ValueError("设置 GATEWAY_MQTT_PASSWORD 时需要同时配置 mqtt.username")

    def _client(self):
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.config["client_id"],
            clean_session=True,
            protocol=mqtt.MQTTv311,
            reconnect_on_failure=False,
        )
        client.connect_timeout = 4.0
        if self.tls is not None:
            client.tls_set_context(self.tls)
        if self.config["username"]:
            client.username_pw_set(self.config["username"], self.password)
        return client

    def _poll(self, client, timeout):
        self.progress = time.monotonic()
        result = client.loop(timeout=timeout)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise BrokerError(mqtt.error_string(result))

    def _await(self, client, complete, what):
        deadline = time.monotonic() + ACK_TIMEOUT
        while not self.stop.is_set() and not complete():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BrokerError(f"{what} 超时")
            self._poll(client, min(remaining, 0.25))
        return complete()

    def _session(self, client, db):
        reason = None

        def on_connect(_client, _userdata, _flags, code, _properties):
            nonlocal reason
            reason = code

        client.on_connect = on_connect
        self.progress = time.monotonic()
        client.connect(self.config["host"], self.config["port"], keepalive=20)
        if not self._await(client, lambda: reason is not None, "CONNACK"):
            return
        if reason.is_failure:
            raise BrokerError(str(reason))
        self.state.update(mqtt_online=True)
        LOG.info("MQTT 已连接")

        while not self.stop.is_set():
            row = outbox.oldest(db)
            if row is None:
                # 无遥测时也处理网络读写，维护 MQTT 的 keepalive 心跳。
                self._poll(client, 0.25)
                continue
            row_id, topic, payload = row
            message = client.publish(topic, payload, qos=1)
            if message.rc != mqtt.MQTT_ERR_SUCCESS:
                raise BrokerError(mqtt.error_string(message.rc))
            # 只有收到 PUBACK 才删除记录。超时后下次连接从数据库队首继续。
            if self._await(client, message.is_published, "PUBACK"):
                outbox.acknowledge(db, row_id)

    def _run(self):
        delay = 1.0
        last_error = None
        with closing(outbox.connect(self.database)) as db:
            while not self.stop.is_set():
                client = self._client()
                try:
                    self._session(client, db)
                except (OSError, BrokerError) as error:
                    if self.state.snapshot()["mqtt_online"]:
                        delay = 1.0
                        last_error = None
                    message = str(error)
                    if message != last_error and not self.stop.is_set():
                        LOG.warning("MQTT 离线，数据保留在本地队列：%s", message)
                        last_error = message
                finally:
                    self.state.update(mqtt_online=False)
                    client.disconnect()
                self.progress = time.monotonic()
                self.stop.wait(delay)
                delay = min(delay * 2, 10.0)

    def run(self):
        try:
            self._run()
        except Exception as error:
            self.error = error
            LOG.exception("MQTT 发送线程异常")
            self.stop.set()
