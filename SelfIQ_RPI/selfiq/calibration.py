"""Comandos, ACK y resultados durables de calibración (Fase 4).

Este módulo no toca MQTT ni HTTP: valida los cuerpos del contrato, mantiene
un almacén SQLite acotado para sobrevivir reinicios del gateway y ofrece el
parser del multipart que envía el nodo a ``/camera/calibration-reference``.
"""
import base64
import hashlib
import json
import logging
import re
import sqlite3
import threading
import time
import zlib
from pathlib import Path

log = logging.getLogger('selfiq.calibration')

WAIT_SECONDS = 10
MAX_ACK_BYTES = 1024
OPERATION_TTL = 24 * 60 * 60
OPERATION_MAX_ROWS = 512
REFERENCE_MAX_BYTES = 2_000_000
REFERENCE_META_MAX_BYTES = 16_384
REFERENCE_SPOOL_MAX_BYTES = 24_000_000
REFERENCE_SPOOL_MAX_ITEMS = 16
REFERENCE_TTL = 24 * 60 * 60

UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
HEX8 = re.compile(r'^[0-9a-fA-F]{8}$')
HEX64 = re.compile(r'^[0-9a-fA-F]{64}$')
TIMESTAMP_RE = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$')
BOUNDARY_RE = re.compile(r'boundary=("?)([A-Za-z0-9._:-]{1,80})\1')

COMMAND_ACTIONS = {
    'calibrate': 'capture',
    'calibration.commit': 'commit',
    'calibration.cancel': 'cancel',
    'calibration.status': 'status',
}


class CalibrationCommandError(ValueError):
    """Error estable que el enlace WSS devuelve al backend."""

    def __init__(self, code, detail):
        super().__init__(detail)
        self.code = code
        self.detail = detail

    def __str__(self):
        return f'{self.code}: {self.detail}'


class ReferenceSpoolFull(RuntimeError):
    pass


def _require(condition, code, detail):
    if not condition:
        raise CalibrationCommandError(code, detail)


def _uuid(value, field):
    _require(isinstance(value, str) and UUID_RE.fullmatch(value),
             'invalid_command', f'{field} inválido')
    return value


def _integer(value, field, minimum=0):
    _require(isinstance(value, int) and not isinstance(value, bool) and value >= minimum,
             'invalid_command', f'{field} inválido')
    return value


def _hex(value, pattern, field):
    _require(isinstance(value, str) and pattern.fullmatch(value),
             'invalid_command', f'{field} inválido')
    return value.lower()


def _timestamp(value, field):
    _require(isinstance(value, str) and TIMESTAMP_RE.fullmatch(value),
             'invalid_command', f'{field} inválido')
    return value


def command_fingerprint(command):
    """Huella semántica estable de una operación.

    ``req_id`` y las fechas pertenecen al intento de transporte y pueden cambiar
    al reintentar la misma ``operation_id``. No deben convertir un reintento
    seguro en ``operation_conflict``.
    """
    stable = {key: value for key, value in command.items()
              if key not in {'req_id', 'issued_at', 'expires_at', '_action'}}
    raw = json.dumps(stable, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(raw).hexdigest()


def validate_command(command):
    """Valida y normaliza un comando de calibración del Anexo A."""
    _require(isinstance(command, dict), 'invalid_command', 'Se requiere un objeto')
    _require(command.get('schema_version') == 1, 'invalid_command',
             'Versión de esquema desconocida')
    kind = command.get('type')
    _require(kind in COMMAND_ACTIONS, 'invalid_command', 'Tipo de comando desconocido')
    action = COMMAND_ACTIONS[kind]
    normalized = dict(command)
    normalized['id'] = _uuid(command.get('id'), 'id')
    normalized['req_id'] = _uuid(command.get('req_id'), 'req_id')
    normalized['branch_id'] = _uuid(command.get('branch_id'), 'branch_id')
    normalized['node_id'] = _uuid(command.get('node_id'), 'node_id')
    normalized['calibration_id'] = _uuid(command.get('calibration_id'), 'calibration_id')
    normalized['roi_id'] = _uuid(command.get('roi_id'), 'roi_id')
    normalized['calibration_version'] = _integer(
        command.get('calibration_version'), 'calibration_version', 1)

    if action in ('capture', 'commit'):
        normalized['config_version'] = _integer(
            command.get('config_version'), 'config_version', 0)
        normalized['context_hash'] = _hex(
            command.get('context_hash'), HEX64, 'context_hash')
        normalized['format_version'] = _integer(
            command.get('format_version'), 'format_version', 1)
        _timestamp(command.get('issued_at'), 'issued_at')
        _timestamp(command.get('expires_at'), 'expires_at')
    elif action == 'cancel':
        normalized['context_hash'] = _hex(
            command.get('context_hash'), HEX64, 'context_hash')
    elif action == 'status':
        if command.get('context_hash') is not None:
            normalized['context_hash'] = _hex(
                command.get('context_hash'), HEX64, 'context_hash')
    _timestamp(command.get('issued_at'), 'issued_at')

    if action == 'capture':
        normalized['capture_id'] = _uuid(command.get('capture_id'), 'capture_id')
        _require(command.get('kind') in ('empty', 'full'), 'invalid_command',
                 'Tipo de referencia desconocido')
    elif action == 'commit':
        normalized['empty_capture_id'] = _uuid(
            command.get('empty_capture_id'), 'empty_capture_id')
        normalized['full_capture_id'] = _uuid(
            command.get('full_capture_id'), 'full_capture_id')
        _require(normalized['empty_capture_id'] != normalized['full_capture_id'],
                 'invalid_command', 'Las capturas de la pareja deben ser distintas')
        normalized['pair_hash'] = _hex(command.get('pair_hash'), HEX64, 'pair_hash')
    normalized.pop('action', None)
    normalized['_action'] = action
    return normalized


def command_action(command):
    return command.get('_action') or COMMAND_ACTIONS.get(command.get('type'))


def _error_present(ack):
    error = ack.get('error')
    if isinstance(error, dict):
        return bool(error.get('code') or error.get('detail'))
    return isinstance(error, str) and bool(error.strip())


def _status_entry(value, field):
    if value is None:
        return
    _require(isinstance(value, dict), 'invalid_ack', f'{field} inválido')
    _require(isinstance(value.get('calibration_version'), int)
             and not isinstance(value.get('calibration_version'), bool)
             and value['calibration_version'] >= 1,
             'invalid_ack', f'{field}.calibration_version inválido')
    if field == 'draft':
        _require(isinstance(value.get('calibration_id'), str) and value['calibration_id'],
                 'invalid_ack', 'draft.calibration_id inválido')
    else:
        _require(isinstance(value.get('context_hash'), str)
                 and HEX64.fullmatch(value['context_hash']),
                 'invalid_ack', f'{field}.context_hash inválido')
    for key in ('empty_crc', 'full_crc'):
        _require(isinstance(value.get(key), str) and HEX8.fullmatch(value[key]),
                 'invalid_ack', f'{field}.{key} inválido')


def validate_ack(command, ack):
    """Valida un ``cal_result`` contra el comando que lo originó.

    Un ACK incompleto o de otra operación/etapa/versión es inválido. Nunca se
    completan campos con los valores solicitados.
    """
    if not isinstance(ack, dict):
        raise CalibrationCommandError('invalid_ack', 'ACK no es un objeto')
    _require(ack.get('schema_version') == 1, 'invalid_ack',
             'Versión de esquema del ACK desconocida')
    _require(ack.get('type') == 'cal_result', 'invalid_ack', 'Tipo de ACK desconocido')
    _require(str(ack.get('id')) == command['id'], 'invalid_ack',
             'El ACK pertenece a otra operación')
    _require(str(ack.get('req_id')) == command['req_id'], 'invalid_ack',
             'El ACK pertenece a otro intento')
    _require(str(ack.get('calibration_id')) == command['calibration_id'],
             'invalid_ack', 'El ACK pertenece a otra calibración')
    _require(str(ack.get('roi_id')) == command['roi_id'], 'invalid_ack',
             'El ACK pertenece a otra zona')
    _require(isinstance(ack.get('calibration_version'), int)
             and not isinstance(ack.get('calibration_version'), bool)
             and ack['calibration_version'] == command['calibration_version'],
             'invalid_ack', 'La versión de calibración no coincide')
    action = command_action(command)
    _require(ack.get('action') == action, 'invalid_ack', 'El ACK corresponde a otra acción')
    _require(isinstance(ack.get('ok'), bool), 'invalid_ack', 'El ACK no trae ok')

    if action in ('capture', 'commit'):
        _require(ack.get('config_version') == command['config_version'],
                 'invalid_ack', 'La configuración no coincide')
        _require(str(ack.get('context_hash')).lower() == command['context_hash'],
                 'invalid_ack', 'La huella del contexto no coincide')
    elif action == 'cancel':
        _require(str(ack.get('context_hash')).lower() == command['context_hash'],
                 'invalid_ack', 'La huella del contexto no coincide')

    if action == 'capture':
        _require(str(ack.get('capture_id')) == command['capture_id'],
                 'invalid_ack', 'El ACK corresponde a otra captura')
        _require(ack.get('kind') == command['kind'], 'invalid_ack',
                 'El ACK corresponde a otra referencia')
        _require(ack.get('format_version') == command['format_version'],
                 'invalid_ack', 'El formato del descriptor no coincide')
        _require(ack.get('status') in ('stored', 'failed', 'unknown'),
                 'invalid_ack', 'Estado de captura desconocido')
        if ack['ok']:
            _require(ack['status'] == 'stored', 'invalid_ack',
                     'Un ACK correcto de captura debe estar stored')
            _require(ack.get('persisted') is True, 'invalid_ack',
                     'El nodo no confirmó la persistencia')
            _require(isinstance(ack.get('descriptor_crc'), str)
                     and HEX8.fullmatch(ack['descriptor_crc']),
                     'invalid_ack', 'El ACK no trae descriptor_crc válido')
            _require(isinstance(ack.get('image_sha256'), str)
                     and HEX64.fullmatch(ack['image_sha256']),
                     'invalid_ack', 'El ACK no trae image_sha256 válido')
            for key in ('image_bytes', 'width', 'height'):
                _require(isinstance(ack.get(key), int)
                         and not isinstance(ack.get(key), bool)
                         and ack[key] >= 1,
                         'invalid_ack', f'El ACK no trae {key} válido')
            _require(isinstance(ack.get('quality'), dict), 'invalid_ack',
                     'El ACK no trae calidad')
            _require(isinstance(ack.get('clock_quality'), str)
                     and bool(ack['clock_quality']), 'invalid_ack',
                     'El ACK no trae clock_quality')
            _require(isinstance(ack.get('uptime_ms'), int)
                     and not isinstance(ack.get('uptime_ms'), bool)
                     and ack['uptime_ms'] >= 0,
                     'invalid_ack', 'El ACK no trae uptime_ms válido')
        else:
            _require(ack.get('status') in ('failed', 'unknown'), 'invalid_ack',
                     'Un ACK fallido de captura debe ser failed/unknown')
            _require(_error_present(ack), 'invalid_ack',
                     'El ACK fallido no explica la causa')
    elif action == 'commit':
        _require(str(ack.get('empty_capture_id')) == command['empty_capture_id'],
                 'invalid_ack', 'Cambió la captura vacía')
        _require(str(ack.get('full_capture_id')) == command['full_capture_id'],
                 'invalid_ack', 'Cambió la captura abastecida')
        _require(ack.get('format_version') == command['format_version'],
                 'invalid_ack', 'El formato del descriptor no coincide')
        _require(ack.get('status') in ('applied', 'rejected', 'failed'),
                 'invalid_ack', 'Estado de confirmación desconocido')
        if ack['ok']:
            _require(ack['status'] == 'applied', 'invalid_ack',
                     'Un ACK correcto de commit debe estar applied')
            _require(isinstance(ack.get('pair_hash'), str)
                     and HEX64.fullmatch(ack['pair_hash']),
                     'invalid_ack', 'El ACK no trae pair_hash válido')
            _require(ack['pair_hash'].lower() == command['pair_hash'],
                     'invalid_ack', 'La huella de la pareja no coincide')
            _require(ack.get('persisted') is True, 'invalid_ack',
                     'El nodo no confirmó la persistencia')
            _require(ack.get('active_calibration_version')
                     == command['calibration_version'],
                     'invalid_ack', 'El nodo activó otra versión')
        else:
            _require(ack['status'] in ('rejected', 'failed'), 'invalid_ack',
                     'Un ACK fallido de commit debe ser rejected/failed')
            _require(_error_present(ack), 'invalid_ack',
                     'El ACK fallido no explica la causa')
    elif action == 'cancel':
        _require(ack.get('status') in ('cancelled', 'failed'), 'invalid_ack',
                 'Estado de cancelación desconocido')
        if ack['ok']:
            _require(ack['status'] == 'cancelled', 'invalid_ack',
                     'Un ACK correcto de cancelación debe estar cancelled')
        else:
            _require(ack['status'] == 'failed', 'invalid_ack',
                     'Un ACK fallido de cancelación debe estar failed')
            _require(_error_present(ack), 'invalid_ack',
                     'El ACK fallido no explica la causa')
    elif action == 'status':
        _require(ack.get('status') == 'reported', 'invalid_ack',
                 'Estado de consulta desconocido')
        _require('active' in ack and 'draft' in ack, 'invalid_ack',
                 'El ACK no trae active/draft')
        _status_entry(ack.get('active'), 'active')
        _status_entry(ack.get('draft'), 'draft')
        results = ack.get('last_results')
        _require(results is None or isinstance(results, list), 'invalid_ack',
                 'last_results inválido')
    return ack


def parse_reference_meta(meta, raw):
    """Valida los metadatos de una referencia y la imagen que los acompaña."""
    _require(isinstance(meta, dict), 'invalid_request', 'Metadatos inválidos')
    _require(raw and len(raw) <= REFERENCE_MAX_BYTES, 'invalid_request',
             'Imagen vacía o demasiado grande')
    _require(raw.startswith(b'\xff\xd8\xff'), 'unsupported_format',
             'La referencia debe ser JPEG')
    required = ('calibration_id', 'calibration_version', 'roi_id', 'capture_id',
                'kind', 'context_hash', 'descriptor_crc', 'format_version',
                'captured_at', 'clock_quality', 'uptime_ms', 'image_sha256',
                'image_bytes', 'width', 'height')
    for key in required:
        _require(key in meta, 'invalid_request', f'Falta {key} en los metadatos')
    for key in ('calibration_id', 'roi_id', 'capture_id'):
        _require(isinstance(meta[key], str) and UUID_RE.fullmatch(meta[key]),
                 'invalid_request', f'{key} inválido')
    _require(meta.get('kind') in ('empty', 'full'), 'invalid_request',
             'Tipo de referencia desconocido')
    _require(isinstance(meta.get('context_hash'), str)
             and HEX64.fullmatch(meta['context_hash']),
             'invalid_request', 'Huella de contexto inválida')
    _require(isinstance(meta.get('descriptor_crc'), str)
             and HEX8.fullmatch(meta['descriptor_crc']),
             'invalid_request', 'CRC del descriptor inválido')
    for key, minimum in (('calibration_version', 1), ('format_version', 1),
                         ('image_bytes', 1), ('width', 1), ('height', 1),
                         ('uptime_ms', 0)):
        _require(isinstance(meta.get(key), int) and not isinstance(meta.get(key), bool)
                 and meta[key] >= minimum,
                 'invalid_request', f'{key} inválido')
    _require(meta['image_bytes'] == len(raw), 'invalid_request',
             'El tamaño de imagen no coincide')
    _require(isinstance(meta.get('image_sha256'), str)
             and HEX64.fullmatch(meta['image_sha256']),
             'invalid_request', 'Hash de imagen inválido')
    digest = hashlib.sha256(raw).hexdigest()
    _require(digest == meta['image_sha256'].lower(), 'invalid_request',
             'El hash de la imagen no coincide')
    _require(isinstance(meta.get('captured_at'), str)
             and TIMESTAMP_RE.fullmatch(meta['captured_at']),
             'invalid_request', 'captured_at inválido')
    _require(isinstance(meta.get('clock_quality'), str)
             and bool(meta['clock_quality']),
             'invalid_request', 'clock_quality inválido')
    descriptor = meta.get('descriptor')
    if descriptor is not None:
        _require(isinstance(descriptor, str), 'invalid_request', 'Descriptor inválido')
        try:
            decoded = base64.b64decode(descriptor, validate=True)
        except (ValueError, TypeError):
            raise CalibrationCommandError('invalid_request',
                                          'El descriptor no es base64 válido') from None
        _require(f'{zlib.crc32(decoded) & 0xffffffff:08x}'
                 == meta['descriptor_crc'].lower(),
                 'invalid_request', 'El descriptor no corresponde al CRC')
    normalized = dict(meta)
    normalized['context_hash'] = meta['context_hash'].lower()
    normalized['descriptor_crc'] = meta['descriptor_crc'].lower()
    normalized['image_sha256'] = digest
    return normalized


def parse_multipart_reference(content_type, raw):
    """Extrae ``meta`` e ``image`` de un multipart/form-data acotado."""
    _require(isinstance(content_type, str)
             and content_type.lower().startswith('multipart/form-data'),
             'invalid_request', 'Se requiere multipart/form-data')
    match = BOUNDARY_RE.search(content_type)
    _require(match is not None, 'invalid_request', 'Falta el boundary')
    boundary = match.group(2).encode()
    _require(len(raw) <= REFERENCE_MAX_BYTES + REFERENCE_META_MAX_BYTES + 4096,
             'invalid_request', 'Solicitud demasiado grande')
    meta_text = None
    image = None
    chunks = raw.split(b'--' + boundary)
    for chunk in chunks[1:]:
        if chunk.startswith(b'--'):
            break
        chunk = chunk[2:] if chunk.startswith(b'\r\n') else chunk
        if chunk.endswith(b'\r\n'):
            chunk = chunk[:-2]
        if not chunk:
            continue
        if b'\r\n\r\n' not in chunk:
            continue
        head, body = chunk.split(b'\r\n\r\n', 1)
        name = None
        for line in head.decode('latin-1', 'replace').split('\r\n'):
            if line.lower().startswith('content-disposition:'):
                found = re.search(r'name="([^"]{1,32})"', line)
                if found:
                    name = found.group(1)
                break
        if name == 'meta':
            _require(body and len(body) <= REFERENCE_META_MAX_BYTES,
                     'invalid_request', 'Metadatos demasiado grandes')
            try:
                meta_text = body.decode('utf-8')
            except UnicodeDecodeError:
                raise CalibrationCommandError('invalid_request',
                                              'Metadatos no UTF-8') from None
        elif name == 'image':
            image = body
    _require(meta_text is not None and image is not None, 'invalid_request',
             'Falta meta o image en el multipart')
    try:
        meta = json.loads(meta_text)
    except (ValueError, TypeError):
        raise CalibrationCommandError('invalid_request',
                                      'Los metadatos no son JSON') from None
    return meta, image


class CalibrationStore:
    """Resultados por operación y spool de referencias, ambos durables."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        try:
            self.path.chmod(0o600)
        except OSError:
            pass
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS calibration_operation (
            operation_id TEXT PRIMARY KEY,
            req_id TEXT NOT NULL,
            action TEXT NOT NULL,
            branch_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            calibration_id TEXT NOT NULL,
            roi_id TEXT NOT NULL,
            calibration_version INTEGER NOT NULL,
            config_version INTEGER,
            context_hash TEXT,
            capture_id TEXT,
            kind TEXT,
            format_version INTEGER,
            command_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            result TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL)''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS calibration_reference (
            capture_id TEXT PRIMARY KEY,
            node_id TEXT NOT NULL,
            branch_id TEXT NOT NULL,
            calibration_id TEXT NOT NULL,
            roi_id TEXT NOT NULL,
            calibration_version INTEGER NOT NULL,
            kind TEXT NOT NULL,
            context_hash TEXT NOT NULL,
            format_version INTEGER NOT NULL,
            meta TEXT NOT NULL,
            image BLOB,
            image_sha256 TEXT NOT NULL,
            image_bytes INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL)''')
        self.db.execute('''CREATE INDEX IF NOT EXISTS calibration_reference_due
                           ON calibration_reference(status, next_attempt)''')
        self.db.commit()

    # -- operaciones ---------------------------------------------------
    def operation(self, operation_id):
        with self.lock:
            row = self.db.execute(
                'SELECT * FROM calibration_operation WHERE operation_id=?',
                (operation_id,)).fetchone()
            return dict(row) if row else None

    def capture_operation(self, capture_id):
        with self.lock:
            row = self.db.execute(
                '''SELECT * FROM calibration_operation
                    WHERE capture_id=? AND action='capture'
                    ORDER BY created_at DESC LIMIT 1''', (capture_id,)).fetchone()
            return dict(row) if row else None

    def begin_operation(self, command, fingerprint):
        now = time.time()
        with self.lock, self.db:
            row = self.db.execute(
                'SELECT * FROM calibration_operation WHERE operation_id=?',
                (command['id'],)).fetchone()
            if row:
                return dict(row)
            self.db.execute(
                '''INSERT INTO calibration_operation
                   (operation_id, req_id, action, branch_id, node_id,
                    calibration_id, roi_id, calibration_version, config_version,
                    context_hash, capture_id, kind, format_version, command_hash,
                    status, result, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (command['id'], command['req_id'], command_action(command),
                 command['branch_id'], command['node_id'], command['calibration_id'],
                 command['roi_id'], command['calibration_version'],
                 command.get('config_version'), command.get('context_hash'),
                 command.get('capture_id'), command.get('kind'),
                 command.get('format_version'), fingerprint, 'preparing', None,
                 now, now))
            self._prune_operations()
            return self.operation(command['id'])

    def operation_status(self, operation_id, status):
        with self.lock, self.db:
            self.db.execute(
                'UPDATE calibration_operation SET status=?, updated_at=? '
                'WHERE operation_id=?', (status, time.time(), operation_id))

    def complete_operation(self, operation_id, ack):
        with self.lock, self.db:
            self.db.execute(
                '''UPDATE calibration_operation
                     SET status='acknowledged', result=?, updated_at=?
                   WHERE operation_id=?''',
                (json.dumps(ack, sort_keys=True, separators=(',', ':')),
                 time.time(), operation_id))

    def _prune_operations(self):
        cutoff = time.time() - OPERATION_TTL
        self.db.execute('DELETE FROM calibration_operation WHERE updated_at < ?', (cutoff,))
        count = self.db.execute(
            'SELECT count(*) FROM calibration_operation').fetchone()[0]
        if count > OPERATION_MAX_ROWS:
            self.db.execute(
                '''DELETE FROM calibration_operation WHERE operation_id IN
                   (SELECT operation_id FROM calibration_operation
                     ORDER BY updated_at ASC LIMIT ?)''',
                (count - OPERATION_MAX_ROWS,))

    # -- referencias ---------------------------------------------------
    def put_reference(self, meta, raw):
        now = time.time()
        canonical = json.dumps(meta, sort_keys=True, separators=(',', ':'))
        with self.lock, self.db:
            row = self.db.execute(
                'SELECT * FROM calibration_reference WHERE capture_id=?',
                (meta['capture_id'],)).fetchone()
            if row:
                same = (row['image_sha256'] == meta['image_sha256']
                        and row['meta'] == canonical)
                if not same:
                    raise CalibrationCommandError(
                        'operation_conflict', 'Esa captura ya tiene otra imagen')
                return 'replay', dict(row)
            used = self.db.execute(
                'SELECT COALESCE(SUM(image_bytes), 0) FROM calibration_reference '
                'WHERE image IS NOT NULL').fetchone()[0]
            count = self.db.execute(
                'SELECT count(*) FROM calibration_reference').fetchone()[0]
            if (used + len(raw) > REFERENCE_SPOOL_MAX_BYTES
                    or count >= REFERENCE_SPOOL_MAX_ITEMS):
                raise ReferenceSpoolFull('Spool de referencias lleno')
            self.db.execute(
                '''INSERT INTO calibration_reference
                   (capture_id, node_id, branch_id, calibration_id, roi_id,
                    calibration_version, kind, context_hash, format_version,
                    meta, image, image_sha256, image_bytes, status, attempts,
                    next_attempt, last_error, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (meta['capture_id'], meta['_node_id'], meta['_branch_id'],
                 meta['calibration_id'], meta['roi_id'],
                 meta['calibration_version'], meta['kind'], meta['context_hash'],
                 meta['format_version'], canonical, raw, meta['image_sha256'],
                 len(raw), 'pending', 0, 0, None, now, now))
            return 'queued', self.reference(meta['capture_id'])

    def reference(self, capture_id):
        with self.lock:
            row = self.db.execute(
                'SELECT * FROM calibration_reference WHERE capture_id=?',
                (capture_id,)).fetchone()
            return dict(row) if row else None

    def due_references(self, limit=4):
        now = time.time()
        with self.lock:
            rows = self.db.execute(
                '''SELECT * FROM calibration_reference
                    WHERE status IN ('pending', 'uploading')
                      AND next_attempt <= ?
                    ORDER BY created_at ASC LIMIT ?''',
                (now, min(max(int(limit), 1), 8))).fetchall()
            return [dict(row) for row in rows]

    def mark_reference_uploaded(self, capture_id):
        with self.lock, self.db:
            self.db.execute(
                '''UPDATE calibration_reference
                     SET status='uploaded', image=NULL, last_error=NULL,
                         updated_at=?
                   WHERE capture_id=?''', (time.time(), capture_id))
            self._prune_references()

    def mark_reference_retry(self, capture_id, error, delay):
        with self.lock, self.db:
            self.db.execute(
                '''UPDATE calibration_reference
                     SET status='pending', attempts=attempts+1,
                         next_attempt=?, last_error=?, updated_at=?
                   WHERE capture_id=?''',
                (time.time() + max(1, delay), str(error)[:300], time.time(),
                 capture_id))

    def mark_reference_terminal(self, capture_id, status, error):
        with self.lock, self.db:
            self.db.execute(
                '''UPDATE calibration_reference
                     SET status=?, image=NULL, last_error=?, updated_at=?
                   WHERE capture_id=?''',
                (status, str(error)[:300], time.time(), capture_id))

    def _prune_references(self):
        cutoff = time.time() - REFERENCE_TTL
        self.db.execute(
            '''DELETE FROM calibration_reference
                WHERE status IN ('uploaded', 'failed', 'conflict')
                  AND updated_at < ?''', (cutoff,))

    def close(self):
        with self.lock:
            self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            self.db.close()


def calibration_store_path(config):
    database = getattr(config, 'offline_db', None)
    if isinstance(database, str) and database:
        return Path(database).with_name('calibration.sqlite3')
    runtime = getattr(config, 'runtime_dir', None) or '/var/lib/selfiq'
    return Path(runtime) / 'calibration.sqlite3'
