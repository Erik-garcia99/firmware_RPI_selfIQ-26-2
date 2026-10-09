"""Wire protocol shared with the current SelfIQ ESP32 firmware."""
from dataclasses import dataclass
import struct

HEADER = 0xABCD
ACK = 0x5433
MAX_DATA = 95
ACTION_NONE = 0
REQ_WIFI = 1
RESP_WIFI = 2
WIFI_DEFAULT = 1
WIFI_ENTERPRISE = 2
FIELD_SSID = 0x69
FIELD_PASSWORD = 0x70
FIELD_USER = 0x75


class ProtocolError(ValueError):
    pass


@dataclass
class Frame:
    header: int
    length: int
    action: int
    frame_type: int
    data: bytes


def _length_from_wire(header: int, length: int) -> int:
    # The ESP32 sends LEN=2 for ACTION+TYPE and uses LEN=FF for NACK.
    if length == 0xFF:
        return 3
    if header == HEADER:
        return length + 3
    if header == ACK:
        return length + 1  # ACK compatibility format: LEN = DATA + 3
    raise ProtocolError("header desconocido")


def parse_frame(packet: bytes) -> Frame:
    if len(packet) < 5:
        raise ProtocolError("trama incompleta")
    header, length = struct.unpack("!HB", packet[:3])
    if length == 0xFF:
        if packet[:5] != struct.pack("!HB", ACK, 0xFF) + b"\xff\x00":
            raise ProtocolError("NACK invalido")
        return Frame(ACK, length, 0xF, 0xF, b"")
    wire_len = _length_from_wire(header, length)
    if len(packet) != wire_len:
        raise ProtocolError("longitud de trama invalida")
    action_type = packet[3]
    return Frame(header, length, action_type & 0x0F, action_type >> 4, packet[4:])


def request_wifi() -> bytes:
    return struct.pack("!HBBB", HEADER, 2, REQ_WIFI, 0)


def _field(identifier: int, value: str) -> bytes:
    raw = value.encode("utf-8")
    if not raw or len(raw) > 255:
        raise ProtocolError("campo vacio o demasiado largo")
    return bytes((identifier, len(raw))) + raw


def credential_response(ssid: str, password: str, user: str | None = None) -> bytes:
    frame_type = WIFI_ENTERPRISE if user is not None else WIFI_DEFAULT
    data = _field(FIELD_SSID, ssid) + _field(FIELD_PASSWORD, password)
    if user is not None:
        data += _field(FIELD_USER, user)
    if len(data) > MAX_DATA:
        raise ProtocolError("credenciales exceden MAX_DATA")
    # Compatibility with tcp_lib.c: it interprets LEN-3 as DATA length.
    length = len(data) + 3
    return struct.pack("!HBB", ACK, length, RESP_WIFI << 4 | frame_type) + data


def nack() -> bytes:
    return struct.pack("!HBBB", ACK, 0xFF, 0xFF, 0)
