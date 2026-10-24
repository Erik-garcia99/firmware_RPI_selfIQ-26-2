"""CRC32 of saved state; detects accidental corruption, not authentication."""
import json
import zlib
from .models import atomic_json

def checksum(value):
    data = json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')
    return f'{zlib.crc32(data) & 0xffffffff:08x}'

def write_state(path, value):
    atomic_json(path, {'version': 1, 'data': value, 'crc32': checksum(value)})

def read_state(path):
    with open(path) as source:
        stored = json.load(source)
    if not isinstance(stored, dict):
        raise ValueError('Estado invalido')
    if 'version' not in stored:
        # Migration is performed only after semantic validation by the caller.
        return stored
    if stored.get('version') != 1 or checksum(stored.get('data')) != stored.get('crc32'):
        raise ValueError('CRC de credenciales invalido')
    return stored['data']
