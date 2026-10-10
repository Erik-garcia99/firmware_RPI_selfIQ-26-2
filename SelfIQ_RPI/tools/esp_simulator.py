#!/usr/bin/env python3
"""Request credentials like ESP32; do not print the password."""
import argparse
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from selfiq.protocol import encode, read_frame, HEADER, ACK

async def main(args):
    r, w = await asyncio.open_connection(args.ip, args.port)
    try:
        request = encode(HEADER, 0, 1, b'\x00')
        if args.fragment:
            for byte in request:
                w.write(bytes([byte]))
                await w.drain()
                await asyncio.sleep(0.05)
        else:
            w.write(request)
            await w.drain()
        frame = await read_frame(r)
        if frame.nack:
            print('NACK: configure y conecte la Raspberry antes de solicitar credenciales.')
            return
        if frame.header != ACK or frame.action != 2 or frame.kind not in (1, 2):
            raise ValueError('Respuesta inesperada')
        pos, fields = 0, {}
        while pos < len(frame.data):
            field, length = frame.data[pos:pos+2]
            pos += 2
            if pos + length > len(frame.data) or field in fields:
                raise ValueError('TLV inválido')
            fields[field] = frame.data[pos:pos+length].decode()
            pos += length
        print('Tipo:', 'Enterprise' if frame.kind == 2 else 'Personal')
        print('SSID:', fields.get(0x69))
        print('Usuario presente:', 0x75 in fields)
        print('Contraseña recibida:', 0x70 in fields)
        w.write(encode(ACK, 3, 0, b'\x01'))
        await w.drain()
    finally:
        w.close()
        await w.wait_closed()

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--ip', default='192.168.50.1')
    p.add_argument('--port', type=int, default=5000)
    p.add_argument('--fragment', action='store_true')
    asyncio.run(main(p.parse_args()))
