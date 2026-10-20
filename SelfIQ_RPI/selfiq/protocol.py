"""Wire protocol: uint16 BE header, uint8 length, type<<4|action, TLVs."""
import asyncio
from dataclasses import dataclass

HEADER = 0xABCD
ACK = 0x5433
NACK = bytes.fromhex('54 33 FF FF 00')
SSID, PASSWORD, USER = 0x69, 0x70, 0x75
MAX_DATA = 100

@dataclass(frozen=True)
class Frame:
    header: int
    kind: int
    action: int
    data: bytes
    nack: bool = False

async def read_frame(reader, timeout=15):
    async def read():
        prefix = await reader.readexactly(3)
        header = int.from_bytes(prefix[:2], 'big')
        length = prefix[2]
        if header not in (HEADER, ACK):
            raise ValueError('Cabecera desconocida')
        if length == 255:
            tail = await reader.readexactly(2)
            if header != ACK or tail != b'\xff\x00':
                raise ValueError('NACK incorrecto')
            return Frame(ACK, 15, 15, b'\x00', True)
        if not 1 <= length <= MAX_DATA + 1:
            raise ValueError('LEN incorrecto')
        body = await reader.readexactly(length)
        return Frame(header, body[0] >> 4, body[0] & 15, body[1:])
    return await asyncio.wait_for(read(), timeout)

def encode(header, kind, action, data):
    if len(data) > MAX_DATA:
        raise ValueError('DATA excede capacidad')
    return header.to_bytes(2, 'big') + bytes([len(data) + 1, kind << 4 | action]) + data

def tlv(field, value):
    data = value.encode('utf-8')
    return bytes([field, len(data)]) + data

def credentials_frame(creds):
    data = tlv(SSID, creds.ssid)
    if creds.kind == 2:
        data += tlv(USER, creds.username)
    data += tlv(PASSWORD, creds.password)
    return encode(ACK, 1, 2, data)
