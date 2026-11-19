"""Linux 串口上的 Modbus RTU 主站。"""

import fcntl
import os
import select
import struct
import termios
import time
import tty

BAUD_RATES = {
    rate: getattr(termios, f"B{rate}")
    for rate in (1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200)
}
TIOCSRS485 = 0x542F
SER_RS485_ENABLED = 1
SER_RS485_RTS_ON_SEND = 2


class ModbusError(Exception):
    """报文超时、校验失败或从站返回异常码。"""


def crc16(data):
    """计算 Modbus CRC16，帧尾以低字节在前的顺序发送。"""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0xA001 if crc & 1 else 0)
    return crc


class ModbusRTU:
    """一个串口仅由一个采集线程使用，依次请求各从站。"""

    def __init__(self, device, baud, timeout, kernel_rs485):
        self.device = device
        self.baud = baud
        self.timeout = timeout
        self.kernel_rs485 = kernel_rs485
        self.fd = None
        # 低速下按 11 bit/字符保守计算帧间静默，高速使用固定 1.75 ms。
        self.gap = 3.5 * 11 / baud if baud <= 19200 else 0.00175
        self.next_frame = 0.0

    def open(self):
        fd = os.open(self.device, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
        try:
            tty.setraw(fd)
            options = termios.tcgetattr(fd)
            options[2] &= ~(termios.CSIZE | termios.PARENB | termios.CSTOPB | termios.CRTSCTS)
            options[2] |= termios.CS8 | termios.CLOCAL | termios.CREAD
            options[4] = options[5] = BAUD_RATES[self.baud]
            termios.tcsetattr(fd, termios.TCSANOW, options)
            if self.kernel_rs485:
                # struct serial_rs485 共 8 个 uint32_t。发送期间 RTS 为高。
                flags = SER_RS485_ENABLED | SER_RS485_RTS_ON_SEND
                config = struct.pack("=8I", flags, *([0] * 7))
                fcntl.ioctl(fd, TIOCSRS485, config)
        except (OSError, termios.error):
            os.close(fd)
            raise
        self.fd = fd

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def _wait(self, reading, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ModbusError("串口事务超时")
        ready = select.select(
            [self.fd] if reading else [], [] if reading else [self.fd], [], remaining
        )
        if not ready[0] and not ready[1]:
            raise ModbusError("串口事务超时")

    def _read_exact(self, size, deadline):
        data = bytearray()
        while len(data) < size:
            self._wait(True, deadline)
            try:
                chunk = os.read(self.fd, size - len(data))
            except BlockingIOError:
                continue
            if not chunk:
                raise OSError("串口连接已关闭")
            data.extend(chunk)
        return bytes(data)

    def read_pair(self, address, register):
        """用功能码 0x03 读取两个 16 位保持寄存器，校验后返回两个原始值。"""
        wait = self.next_frame - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        request = struct.pack(">BBHH", address, 3, register, 2)
        request += struct.pack("<H", crc16(request))
        deadline = time.monotonic() + self.timeout
        try:
            termios.tcflush(self.fd, termios.TCIFLUSH)
            offset = 0
            while offset < len(request):
                self._wait(False, deadline)
                try:
                    offset += os.write(self.fd, request[offset:])
                except BlockingIOError:
                    continue
            header = self._read_exact(3, deadline)
            if header[0] != address:
                raise ModbusError(f"从站地址不匹配 ({header[0]})")
            if header[1] == 0x83:
                frame = header + self._read_exact(2, deadline)
            elif header[1:] == bytes((3, 4)):
                frame = header + self._read_exact(6, deadline)
            else:
                raise ModbusError("响应功能码或字节数错误")
            if crc16(frame[:-2]) != int.from_bytes(frame[-2:], "little"):
                raise ModbusError("CRC16 校验失败")
            if header[1] == 0x83:
                raise ModbusError(f"从站异常码 0x{header[2]:02x}")
            return struct.unpack(">HH", frame[3:7])
        finally:
            self.next_frame = time.monotonic() + self.gap
