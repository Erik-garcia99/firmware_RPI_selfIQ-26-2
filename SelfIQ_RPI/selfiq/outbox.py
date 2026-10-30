"""Durable local inbox/outbox. No cloud deletion without an explicit receipt.

The adapter send_batch is deliberately not connected until the backend contract
is implemented. It must return IDs durably accepted, including deduplication.
"""
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import time

class StorageFull(RuntimeError):
    pass

class Outbox:
    def __init__(self, config, disk_usage=shutil.disk_usage):
        self.c, self.disk_usage = config, disk_usage
        self.path = Path(config.offline_db)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        os.chmod(self.path, 0o600)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA wal_autocheckpoint=128')
        self.db.execute('PRAGMA journal_size_limit=1048576')
        self.db.execute('''CREATE TABLE IF NOT EXISTS events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL, topic TEXT NOT NULL,
            payload TEXT NOT NULL, received_at REAL NOT NULL)''')
        self.db.commit()

    def size(self):
        return sum(p.stat().st_size for p in
                   (self.path, Path(str(self.path)+'-wal'), Path(str(self.path)+'-shm'))
                   if p.exists())

    def put(self, topic, payload):
        if len(payload) > self.c.offline_max_payload:
            raise ValueError('Payload demasiado grande')
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError('Se requiere objeto JSON')
        event_id = data.get('event_id')
        if not isinstance(event_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,96}', event_id):
            raise ValueError('event_id invalido')
        if not isinstance(data.get('event_type'), str) or not re.fullmatch(r'[a-z0-9_]{1,32}', data['event_type']):
            raise ValueError('event_type invalido')
        if 'data' not in data:
            raise ValueError('Falta data')
        # Reject NaN, Infinity and malformed JSON, preserve original payload for equality.
        json.dumps(data, allow_nan=False)
        raw = payload.decode('utf-8') if isinstance(payload, bytes) else payload
        with self.lock:
            old = self.db.execute('SELECT topic,payload FROM events WHERE event_id=?', (event_id,)).fetchone()
            if old:
                if old != (topic, raw):
                    raise ValueError('event_id reutilizado con contenido distinto')
                return event_id
            usage = self.disk_usage(self.path.parent)
            reserve = max(self.c.offline_reserve_bytes,
                          int(usage.total * self.c.offline_reserve_fraction))
            # Conservative headroom for btree + WAL pages, not just payload bytes.
            headroom = max(65536, len(payload) * 8)
            free_pages = self.db.execute('PRAGMA freelist_count').fetchone()[0]
            page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
            logical_used = (self.db.execute('PRAGMA page_count').fetchone()[0] - free_pages) * page_size
            wal_path = Path(str(self.path) + '-wal')
            wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
            if (usage.free - headroom < reserve or
                    logical_used + wal_bytes + headroom > usage.total * self.c.offline_max_fraction):
                raise StorageFull('STORAGE_FULL: cuota/reserva alcanzada')
            with self.db:
                self.db.execute('INSERT INTO events(event_id,topic,payload,received_at) VALUES(?,?,?,?)',
                                (event_id, topic, raw, time.time()))
        return event_id  # ONLY after COMMIT has completed

    def batch(self, limit=100):
        with self.lock:
            rows = self.db.execute('SELECT event_id,topic,payload,received_at FROM events ORDER BY seq LIMIT ?',
                                   (min(max(int(limit), 1), 100),)).fetchall()
        return [dict(event_id=r[0], topic=r[1], payload=json.loads(r[2]), received_at=r[3]) for r in rows]

    def acknowledge(self, ids):
        with self.lock, self.db:
            self.db.executemany('DELETE FROM events WHERE event_id=?', ((i,) for i in ids))

    def replay_once(self, send_batch, limit=100):
        batch = self.batch(limit)
        if not batch:
            return 0
        accepted = send_batch(batch)  # exception / lost reply: keep the entire batch
        offered = {item['event_id'] for item in batch}
        if not isinstance(accepted, (list, set, tuple)) or any(not isinstance(i, str) or i not in offered for i in accepted):
            raise ValueError('Confirmacion backend invalida')
        self.acknowledge(accepted)
        return len(set(accepted))

    def status(self):
        with self.lock:
            count = self.db.execute('SELECT count(*) FROM events').fetchone()[0]
            return {'pending': count, 'bytes': self.size(), 'cloud_sync': 'not_configured'}

    def close(self):
        with self.lock:
            self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            self.db.close()
