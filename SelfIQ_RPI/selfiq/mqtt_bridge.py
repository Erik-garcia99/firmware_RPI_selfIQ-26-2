"""MQTT local -> alta REST firmada -> respuesta MQTT individual (sin tocar STA/AP)."""
import asyncio
import base64
import hashlib
import json
import logging
import math
import re
import secrets
import time
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import paho.mqtt.client as mqtt

from .backend_config import websocket_url
from .outbox import Outbox, StorageFull
from .node_control import (reset_frame, parse_reset_result,
                           occupancy_adjust_frame, parse_occupancy_adjust_result)
from .calibration import (
    CalibrationCommandError,
    CalibrationStore,
    ReferenceSpoolFull,
    calibration_store_path,
    command_action,
    command_fingerprint,
    parse_reference_meta,
    validate_ack,
    validate_command,
    WAIT_SECONDS as CALIBRATION_WAIT_SECONDS,
)

log = logging.getLogger('selfiq.mqtt')
MAC_RE = re.compile(r'^(?:[0-9A-F]{2}:){5}[0-9A-F]{2}$')
UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
REQUEST = 'selfiq/esp32_setup'
NODE_TYPES = {'guide', 'access_counter', 'shelf_cam', 'environment'}
NODE_TYPE_ALIASES = {'shelf_guide': 'guide', 'occupancy': 'access_counter', 'camera': 'shelf_cam'}

GUIDE_MODULE = 'shelf_monitor'
GUIDE_BOOT_RE = re.compile(r'^[0-9a-f]{8}$')
GUIDE_APPLIED = {'on', 'off', 'rejected', 'error'}
GUIDE_REASONS = {'ok', 'stale', 'wrong_boot', 'bad_channel', 'malformed',
                 'duplicate', 'gpio_error', 'ttl', 'busy'}
GUIDE_MAX_WAIT_SECONDS = 65
GUIDE_STATUS_WAIT_SECONDS = 2
GUIDE_SAMPLE_MAX_AGE_SECONDS = 30


class MQTTBridge:
    def __init__(self, config, backend):
        self.config = config
        self.backend = backend
        self.client = None
        self.connected = False
        self.queue = asyncio.Queue(maxsize=64)
        self.worker = None
        self.loop = None
        self.pending = set()
        self.outbox = None
        self.storage_error = ''
        self.reset_waiters = {}
        self.occupancy_reset_waiters = {}
        self.reset_lock = threading.Lock()
        self._roi_waiters = {}
        self._roi_lock = threading.Lock()
        self.cal_store = None
        self._cal_waiters = {}
        self._cal_lock = threading.Lock()
        self._cal_store_lock = threading.Lock()
        self._calibration_worker = None
        self.subscriptions_ready = threading.Event()
        self.subscription_mid = None
        self.default_node_type = 'guide'  # Firmware actual: shelf guide; configurar para otros tipos.
        self._guide_waiters = {}
        self._guide_lock = threading.Lock()
        self._guide_samples = {}
        self._guide_capabilities = {}
        self._guide_capabilities_pending = {}
        self._guide_capabilities_confirmed = {}
        self._guide_capabilities_wake = asyncio.Event()
        self._guide_capabilities_worker = None

    def _api_url(self, suffix):
        base = urlsplit(websocket_url(self.config.backend_ws_url))
        scheme = 'https' if base.scheme == 'wss' else 'http'
        prefix = (base.path.removesuffix('/ws/gateways') + '/gateways/'
                  + quote(self.config.gateway_id, safe=''))
        return urlunsplit((scheme, base.netloc, prefix + suffix, '', ''))

    def endpoint(self):
        return self._api_url('/nodes/register')

    def _post_signed(self, url, payload):
        request = Request(url, json.dumps(payload).encode(),
                          {'Content-Type': 'application/json'}, method='POST')
        with urlopen(request, timeout=10) as response:
            if response.status != 200:
                raise RuntimeError('Backend devolvió respuesta inesperada')
            return json.loads(response.read(2048))

    def _signed_post(self, mac, node_type):
        identity = self.backend.ensure_identity()
        timestamp = int(time.time())
        nonce = secrets.token_urlsafe(18)
        data = ('selfiq-node-register-v1\n' + self.config.gateway_id + '\n'
                + str(timestamp) + '\n' + nonce + '\n' + mac + '\n' + node_type).encode()
        payload = {'mac_address': mac, 'node_type': node_type, 'timestamp': timestamp,
                   'nonce': nonce, 'signature': base64.b64encode(identity.key.sign(data)).decode()}
        request = Request(self.endpoint(), json.dumps(payload).encode(),
                          {'Content-Type': 'application/json'}, method='POST')
        with urlopen(request, timeout=10) as response:
            if response.status != 200:
                raise RuntimeError('Backend devolvió respuesta inesperada')
            return json.loads(response.read(2048))

    def _signed_heartbeat(self, node_id):
        identity = self.backend.ensure_identity()
        timestamp = int(time.time())
        nonce = secrets.token_urlsafe(18)
        data = ('selfiq-node-heartbeat-v1\n' + self.config.gateway_id + '\n'
                + str(timestamp) + '\n' + nonce + '\n' + node_id).encode()
        payload = {'timestamp': timestamp, 'nonce': nonce,
                   'signature': base64.b64encode(identity.key.sign(data)).decode()}
        return self._post_signed(
            self._api_url('/nodes/' + quote(node_id, safe='') + '/heartbeat'), payload)

    def authorize_node(self, branch_id, node_id):
        """Consulta vigente, sin caché permisiva ni actualización de presencia.

        None significa rechazo explícito. Errores de red/autenticación propagan
        para conservar la cola y bloquear el envío, nunca autorizar por defecto.
        """
        if not all(isinstance(v, str) and UUID_RE.fullmatch(v) for v in (branch_id, node_id)):
            return None
        if not self.backend.connected or self.backend.registration_status != 'registered':
            raise RuntimeError('Gateway sin autorización vigente')
        if branch_id != self.backend.branch_id:
            return None
        timestamp = int(time.time())
        nonce = secrets.token_urlsafe(18)
        data = (f'selfiq-node-authorize-v1\n{self.config.gateway_id}\n'
                f'{timestamp}\n{nonce}\n{node_id}').encode()
        payload = {'timestamp': timestamp, 'nonce': nonce,
                   'signature': base64.b64encode(self.backend.ensure_identity().key.sign(data)).decode()}
        response = self._post_signed(
            self._api_url('/nodes/' + quote(node_id, safe='') + '/authorize'), payload)
        if (not isinstance(response, dict) or response.get('id') != node_id
                or response.get('branch_id') != branch_id
                or not isinstance(response.get('authorized'), bool)):
            raise ValueError('Respuesta de autorización inválida')
        if not response['authorized']:
            return None
        registered_at = response.get('registered_at')
        if (isinstance(registered_at, bool) or not isinstance(registered_at, (int, float))
                or not math.isfinite(registered_at)):
            raise ValueError('Fecha de registro inválida')
        return registered_at

    async def _send_heartbeat(self, node_id):
        try:
            await asyncio.to_thread(self._signed_heartbeat, node_id)
        except (URLError, OSError, TimeoutError, ValueError, KeyError, RuntimeError) as exc:
            log.warning('Presencia de nodo %s no registrada (%s)', node_id, type(exc).__name__)
        except Exception:
            log.exception('Error inesperado al actualizar presencia de %s', node_id)

    def _queue_heartbeat(self, node_id):
        if self.loop is not None:
            self.loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(self._send_heartbeat(node_id)))
            caps = self._guide_capabilities.get(node_id)
            if caps:
                self._queue_guide_capabilities(node_id, caps)

    # --- Guía LED (GL-3): resultado real del nodo, nunca PUBACK ---
    def _guide_key(self, branch_id, node_id, module=GUIDE_MODULE):
        return f'{branch_id}/{node_id}/{module}'

    def _guide_record_sample(self, branch_id, node_id, module, boot, uptime_ms):
        """Guarda una muestra de uptime/boot del nodo, con sello monotónico local."""
        if not (isinstance(boot, str) and GUIDE_BOOT_RE.fullmatch(boot)
                and isinstance(uptime_ms, int) and not isinstance(uptime_ms, bool)
                and 0 <= uptime_ms <= 2**32 - 1):
            return False
        with self._guide_lock:
            self._guide_samples[self._guide_key(branch_id, node_id, module)] = {
                'boot': boot, 'uptime_ms': uptime_ms, 'at': time.monotonic()}
        return True

    def _guide_uptime(self, branch_id, node_id, module=GUIDE_MODULE):
        with self._guide_lock:
            sample = self._guide_samples.get(self._guide_key(branch_id, node_id, module))
        if not sample:
            return None
        age = time.monotonic() - sample['at']
        if age < 0 or age > GUIDE_SAMPLE_MAX_AGE_SECONDS:
            return None
        return sample['boot'], sample['uptime_ms'] + max(0, int(age * 1000))

    def _guide_publish(self, topic, payload):
        if not self.connected or self.client is None:
            raise ValueError('MQTT local no conectado')
        info = self.client.publish(topic, payload, qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f'Publicación MQTT fallida rc={info.rc}')
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise RuntimeError('El broker no confirmó la publicación MQTT')

    def _guide_register(self, waiter):
        with self._guide_lock:
            if waiter['id'] in self._guide_waiters:
                raise ValueError('ID de guía duplicado')
            self._guide_waiters[waiter['id']] = waiter

    def _guide_finish(self, command_id):
        with self._guide_lock:
            return self._guide_waiters.pop(command_id, None)

    def notify_guide_result(self, payload, branch_id, node_id, module):
        """Entrega ``guide.result``/``guide.state`` solo a su espera correlacionada."""
        if module != GUIDE_MODULE or not isinstance(payload, dict):
            return False
        kind = payload.get('t')
        if kind not in ('guide.result', 'guide.state'):
            return False
        command_id = payload.get('id')
        if not isinstance(command_id, str) or not 8 <= len(command_id) <= 64:
            return False
        if not self._guide_record_sample(branch_id, node_id, module,
                                         payload.get('boot'), payload.get('uptime_ms')):
            log.warning('guía: resultado con boot/uptime inválido descartado')
            return False
        if kind == 'guide.result' and (payload.get('applied') not in GUIDE_APPLIED
                                       or payload.get('reason') not in GUIDE_REASONS):
            log.warning('guía: resultado con applied/reason inválido descartado')
            return False
        with self._guide_lock:
            waiter = self._guide_waiters.get(command_id)
            if waiter is None and kind == 'guide.result':
                # light.test: el firmware genera su propio id, se correlaciona
                # por canal mientras haya una sola prueba activa en ese canal.
                for candidate in self._guide_waiters.values():
                    if (candidate['kind'] == 'test'
                            and candidate['channel'] == payload.get('ch')):
                        waiter = candidate
                        break
        if not waiter or waiter['branch_id'] != branch_id or waiter['node_id'] != node_id:
            log.warning('guía: resultado ajeno o sin espera descartado')
            return False
        if waiter['kind'] == 'test':
            # El firmware genera su propio id para light.test: se correlaciona
            # por canal, con una sola espera activa por canal.
            if kind != 'guide.result' or payload.get('ch') != waiter['channel']:
                return False
        else:
            if kind != waiter['expect']:
                return False
            if waiter.get('session') and payload.get('session') != waiter['session']:
                log.warning('guía: resultado de otra sesión descartado')
                return False
            if kind == 'guide.result' and payload.get('ch') != waiter['channel']:
                return False
        waiter['result'] = payload
        waiter['event'].set()
        return True

    def _guide_request_state(self, branch_id, node_id, module=GUIDE_MODULE):
        """Consulta ``guide.status`` para obtener un uptime medido, no inventado."""
        if not self.connected or not self.subscriptions_ready.wait(5):
            raise ValueError('MQTT no está listo para consultar el estado de la guía')
        command_id = f'gs-{secrets.token_hex(8)}'
        waiter = {'id': command_id, 'kind': 'status', 'expect': 'guide.state',
                  'event': threading.Event(), 'result': None,
                  'branch_id': branch_id, 'node_id': node_id, 'session': None,
                  'channel': None}
        self._guide_register(waiter)
        try:
            topic = f'selfiq/{branch_id}/{node_id}/{module}/command'
            payload = json.dumps({'v': 1, 't': 'guide.status', 'id': command_id},
                                 separators=(',', ':'))
            self._guide_publish(topic, payload)
            if not waiter['event'].wait(GUIDE_STATUS_WAIT_SECONDS):
                return None
            return waiter['result']
        finally:
            self._guide_finish(command_id)

    def _guide_deadline(self, branch_id, node_id):
        sample = self._guide_uptime(branch_id, node_id)
        if sample is None:
            state = self._guide_request_state(branch_id, node_id)
            if not state:
                raise ValueError('guide_node_unavailable')
            sample = self._guide_uptime(branch_id, node_id)
            if sample is None:
                raise ValueError('guide_node_unavailable')
        return sample

    def _guide_outcome(self, payload, channel, session, boot):
        if payload is None:
            return {'delivery': 'unknown', 'light': 'unknown', 'channel': channel,
                    'session_id': session, 'node_boot': boot, 'reason': 'node_timeout'}
        applied = payload.get('applied')
        if applied in ('on', 'off'):
            delivery, light = 'applied', applied
        elif applied == 'rejected':
            delivery, light = 'rejected', 'unknown'
        else:
            delivery, light = 'error', 'unknown'
        result = {'delivery': delivery, 'light': light, 'channel': channel,
                  'session_id': session, 'node_boot': boot}
        if delivery != 'applied' and payload.get('reason') in GUIDE_REASONS:
            result['reason'] = payload['reason']
        return result

    def _guide_command(self, message, action):
        node_id, branch_id = message.get('node_id'), message.get('branch_id')
        module = message.get('module') or GUIDE_MODULE
        session = message.get('session_id')
        channel = message.get('channel')
        if (not isinstance(node_id, str) or not UUID_RE.fullmatch(node_id)
                or not isinstance(branch_id, str) or not UUID_RE.fullmatch(branch_id)
                or module != GUIDE_MODULE
                or not isinstance(session, str) or not UUID_RE.fullmatch(session)
                or not isinstance(channel, int) or isinstance(channel, bool)
                or not 0 <= channel < 10):
            raise ValueError('Orden de guía inválida')
        caps = self._guide_capabilities.get(node_id)
        if caps and channel >= caps['channels']:
            raise ValueError('invalid_channel')
        ttl_ms = message.get('ttl_ms', 60000)
        accept_window_ms = message.get('accept_window_ms', 3000)
        if (not isinstance(ttl_ms, int) or isinstance(ttl_ms, bool) or not 1000 <= ttl_ms <= 60000
                or not isinstance(accept_window_ms, int) or isinstance(accept_window_ms, bool)
                or not 0 <= accept_window_ms <= 3000):
            raise ValueError('Plazo de guía inválido')
        boot, uptime = self._guide_deadline(branch_id, node_id)
        accept_ms = uptime + accept_window_ms
        command_id = f'gd-{secrets.token_hex(8)}'
        # El contrato §7.2 admite exactamente seis claves en guide.stop: un
        # `accept_ms` extra hace que el firmware lo rechace como malformado y
        # no emita resultado (el gateway agota su espera y el apagado queda
        # sin confirmar). Solo guide.start lleva plazos.
        wire = {'v': 1, 't': action, 'id': command_id, 'session': session,
                'ch': channel, 'boot': boot}
        if action == 'guide.start':
            wire['accept_ms'] = accept_ms
            wire['end_ms'] = accept_ms + ttl_ms
        waiter = {'id': command_id, 'kind': 'start', 'expect': 'guide.result',
                  'event': threading.Event(), 'result': None, 'unknown': False,
                  'branch_id': branch_id, 'node_id': node_id, 'session': session,
                  'channel': channel}
        self._guide_register(waiter)
        try:
            topic = f'selfiq/{branch_id}/{node_id}/{module}/command'
            self._guide_publish(topic, json.dumps(wire, separators=(',', ':')))
            log.info('Guía %s publicada: nodo=%s ch=%s id=%s', action, node_id, channel, command_id)
            if waiter.get('unknown') or not waiter['event'].wait(GUIDE_MAX_WAIT_SECONDS):
                log.warning('Guía %s sin resultado: id=%s', action, command_id)
                return self._guide_outcome(None, channel, session, boot)
            outcome = self._guide_outcome(waiter['result'], channel, session, boot)
            log.info('Guía %s resultado: id=%s ch=%s entrega=%s luz=%s%s', action,
                     command_id, channel, outcome['delivery'], outcome['light'],
                     f" motivo={outcome['reason']}" if outcome.get('reason') else '')
            return outcome
        finally:
            self._guide_finish(command_id)

    def guide_start(self, message):
        return self._guide_command(message or {}, 'guide.start')

    def guide_stop(self, message):
        return self._guide_command(message or {}, 'guide.stop')

    def guide_status(self, message):
        message = message or {}
        node_id, branch_id = message.get('node_id'), message.get('branch_id')
        if (not isinstance(node_id, str) or not UUID_RE.fullmatch(node_id)
                or not isinstance(branch_id, str) or not UUID_RE.fullmatch(branch_id)):
            raise ValueError('Identidad del nodo inválida')
        state = self._guide_request_state(branch_id, node_id)
        if not state:
            return {'delivery': 'unknown', 'reason': 'node_timeout'}
        return {'delivery': 'applied', 'boot': state.get('boot'),
                'uptime_ms': state.get('uptime_ms'), 'on': state.get('on'),
                'last_ch': state.get('last_ch'), 'last': state.get('last')}

    def test_light(self, message):
        """Prueba staff: espera el ``guide.result`` del nodo, no el PUBACK."""
        node_id = message.get('node_id')
        branch_id = message.get('branch_id')
        module = message.get('module') or GUIDE_MODULE
        channel = message.get('channel')
        duration_ms = message.get('duration_ms', 5000)
        if (not isinstance(node_id, str) or not UUID_RE.fullmatch(node_id)
                or not isinstance(branch_id, str) or not UUID_RE.fullmatch(branch_id)
                or module != GUIDE_MODULE
                or not isinstance(channel, int) or isinstance(channel, bool) or not 0 <= channel < 10
                or not isinstance(duration_ms, int) or isinstance(duration_ms, bool)
                or not 100 <= duration_ms <= 30000):
            raise ValueError('Orden de prueba de luz inválida')
        caps = self._guide_capabilities.get(node_id)
        if caps and channel >= caps['channels']:
            raise ValueError('invalid_channel')
        if not self.connected or not self.subscriptions_ready.wait(5):
            raise ValueError('MQTT no está listo para probar la luz')
        session = '00000000-0000-4000-8000-' + secrets.token_hex(6)
        waiter = {'id': 'lt-' + secrets.token_hex(8), 'kind': 'test',
                  'event': threading.Event(), 'result': None, 'unknown': False,
                  'branch_id': branch_id, 'node_id': node_id, 'session': None,
                  'channel': channel}
        with self._guide_lock:
            self._guide_waiters[waiter['id']] = waiter
        try:
            topic = f'selfiq/{branch_id}/{node_id}/{module}/command'
            self._guide_publish(topic, json.dumps(
                {'type': 'light.test', 'channel': channel, 'duration_ms': duration_ms},
                separators=(',', ':')))
            if waiter.get('unknown') or not waiter['event'].wait(GUIDE_MAX_WAIT_SECONDS):
                return {'status': 'unknown', 'light': 'unknown', 'channel': channel,
                        'durationSeconds': duration_ms / 1000, 'expiresInSeconds': 0}
            applied = (waiter['result'] or {}).get('applied')
            if applied in ('on', 'rejected', 'error'):
                status = {'on': 'applied', 'rejected': 'rejected', 'error': 'unknown'}[applied]
                light = 'on' if applied == 'on' else 'unknown'
            else:
                status, light = 'unknown', 'unknown'
            return {'status': status, 'light': light, 'channel': channel,
                    'durationSeconds': duration_ms / 1000,
                    'expiresInSeconds': duration_ms / 1000}
        finally:
            self._guide_finish(waiter['id'])

    def _validate_guide_capabilities(self, payload, branch_id):
        if (not isinstance(payload, dict) or payload.get('v') != 1
                or payload.get('t') != 'guide.capabilities'):
            return None
        boot = payload.get('boot')
        driver = payload.get('driver')
        channels = payload.get('channels')
        channels_map = payload.get('channels_map')
        if not (isinstance(boot, str) and GUIDE_BOOT_RE.fullmatch(boot)
                and driver in ('direct', 'hc595')
                and isinstance(channels, int) and not isinstance(channels, bool)
                and 1 <= channels <= 8 and isinstance(channels_map, list)
                and len(channels_map) == channels):
            return None
        if any(not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 255
               for value in channels_map) or len(set(channels_map)) != channels:
            return None
        return {'boot': boot, 'driver': driver, 'channels': channels,
                'channels_map': channels_map, 'fw': payload.get('fw'),
                '_branch_id': branch_id}

    def _queue_guide_capabilities(self, node_id, caps):
        if self.loop is None:
            return
        def enqueue():
            confirmed = self._guide_capabilities_confirmed.get(node_id)
            if confirmed and confirmed[0] == caps and time.monotonic() - confirmed[1] < 60:
                return
            self._guide_capabilities_pending[node_id] = caps
            self._guide_capabilities_wake.set()
        self.loop.call_soon_threadsafe(enqueue)

    def _receive_guide_capabilities(self, payload, branch_id, node_id, module):
        # El módulo de cámara no declara ni configura canales LED.
        if module != GUIDE_MODULE:
            return
        caps = self._validate_guide_capabilities(payload, branch_id)
        if not caps:
            log.warning('Capacidades de guía inválidas: nodo=%s', node_id)
            return
        if self._guide_capabilities.get(node_id) != caps:
            log.info('Capacidades de guía recibidas: nodo=%s canales=%s driver=%s',
                     node_id, caps['channels'], caps['driver'])
        self._guide_capabilities[node_id] = caps
        self._queue_guide_capabilities(node_id, caps)

    async def _flush_guide_capabilities(self):
        if not self.backend.connected:
            return
        for node_id, caps in list(self._guide_capabilities_pending.items()):
            if await asyncio.to_thread(self._post_guide_capabilities, node_id, caps):
                self._guide_capabilities_confirmed[node_id] = (caps, time.monotonic())
                # No descartar un anuncio nuevo recibido durante el HTTP.
                if self._guide_capabilities_pending.get(node_id) is caps:
                    self._guide_capabilities_pending.pop(node_id, None)

    async def _guide_capability_uploads(self):
        while True:
            self._guide_capabilities_wake.clear()
            await self._flush_guide_capabilities()
            try:
                await asyncio.wait_for(self._guide_capabilities_wake.wait(), 5)
            except asyncio.TimeoutError:
                pass

    def _post_guide_capabilities(self, node_id, caps):
        """Registra metadatos del nodo; nunca encola órdenes de encendido."""
        if not self.backend.connected or caps.get('_branch_id') != self.backend.branch_id:
            return False
        try:
            identity = self.backend.ensure_identity()
            timestamp = int(time.time())
            nonce = secrets.token_urlsafe(18)
            map_csv = ','.join(str(value) for value in caps['channels_map'])
            data = (f'selfiq-guide-capabilities-v1\n{self.config.gateway_id}\n{timestamp}\n'
                    f'{nonce}\n{node_id}\n{caps["boot"]}\n{caps["driver"]}\n'
                    f'{caps["channels"]}\n{map_csv}').encode()
            payload = {'timestamp': timestamp, 'nonce': nonce,
                       'signature': base64.b64encode(identity.key.sign(data)).decode(),
                       'boot': caps['boot'], 'driver': caps['driver'],
                       'channels': caps['channels'], 'channels_map': caps['channels_map']}
            url = self._api_url('/nodes/' + quote(node_id, safe='') + '/guide-capabilities')
            result = self._post_signed(url, payload)
            if (not isinstance(result, dict) or result.get('ok') is not True
                    or result.get('node_id') != node_id or result.get('channels') != caps['channels']):
                raise ValueError('El backend no confirmó las capacidades del nodo')
            log.info('Capacidades de guía reenviadas: nodo=%s canales=%s', node_id, caps['channels'])
            return True
        except HTTPError as exc:
            detail = ''
            try:
                body = json.loads(exc.read(2048))
                if isinstance(body, dict) and isinstance(body.get('detail'), str):
                    detail = body['detail'][:200].replace('\n', ' ')
            except (ValueError, OSError, UnicodeError):
                pass
            log.warning('Capacidades de guía pendientes: nodo=%s HTTP=%s%s; se reintentará',
                        node_id, exc.code, f' ({detail})' if detail else '')
            exc.close()
        except Exception as exc:
            log.warning('Capacidades de guía pendientes: nodo=%s error=%s; se reintentará',
                        node_id, type(exc).__name__)
        return False

    def _clear_guide_waiters(self):
        with self._guide_lock:
            waiters = list(self._guide_waiters.values())
            self._guide_waiters.clear()
        for waiter in waiters:
            waiter['result'] = None
            waiter['unknown'] = True
            waiter['event'].set()

    def _request_guide_states(self):
        """Al reconectar, recupera el último motivo de apagado de cada nodo guía."""
        for node_id, caps in list(self._guide_capabilities.items()):
            try:
                self._guide_request_state(caps.get('_branch_id'), node_id)
            except Exception:
                log.warning('No se pudo consultar guide.state de %s al reconectar', node_id)

    def reset_node(self, message):
        node_id, branch_id = message.get('node_id'), message.get('branch_id')
        if not all(isinstance(value, str) and UUID_RE.fullmatch(value)
                   for value in (node_id, branch_id)):
            raise ValueError('Identidad del nodo inválida')
        if not self.connected or not self.subscriptions_ready.wait(5):
            raise ValueError('MQTT no está listo para recibir la confirmación del nodo')
        topic = f'selfiq/{branch_id}/{node_id}/control'
        waiter = {'event': threading.Event(), 'topic': topic + '/result', 'result': None}
        with self.reset_lock:
            if waiter['topic'] in self.reset_waiters:
                raise ValueError('Ya hay un borrado pendiente para este nodo')
            self.reset_waiters[waiter['topic']] = waiter
        try:
            log.info('Borrado de identidad solicitado: nodo=%s tópico=%s/command payload=01 03', node_id, topic)
            info = self.client.publish(topic + '/command', reset_frame(), qos=1, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('No se pudo enviar el restablecimiento')
            if not waiter['event'].wait(20):
                log.warning('Sin confirmación de borrado: nodo=%s tópico esperado=%s', node_id, waiter['topic'])
                raise ValueError('El nodo no confirmó el borrado de identidad; comprueba que esté conectado y tenga firmware actualizado')
            if waiter['result'] is not True:
                raise ValueError('El nodo no pudo borrar su identidad de NVS')
            log.info('Borrado de identidad confirmado por nodo=%s', node_id)
            return {'identity_reset': True, 'node_id': node_id}
        finally:
            with self.reset_lock:
                self.reset_waiters.pop(waiter['topic'], None)

    def adjust_occupancy(self, message):
        node_id, branch_id = message.get('node_id'), message.get('branch_id')
        value = message.get('value', 0)
        if not isinstance(value, int) or value < 0:
            raise ValueError('Valor de aforo inválido')
        if not all(isinstance(v, str) and UUID_RE.fullmatch(v)
                   for v in (node_id, branch_id)):
            raise ValueError('Identidad del nodo inválida')
        if not self.connected or not self.subscriptions_ready.wait(5):
            raise ValueError('MQTT no está listo para recibir la confirmación del nodo')
        topic = f'selfiq/{branch_id}/{node_id}/control'
        waiter = {'event': threading.Event(), 'topic': topic + '/result', 'result': None}
        with self.reset_lock:
            if waiter['topic'] in self.occupancy_reset_waiters:
                raise ValueError('Ya hay un ajuste de aforo pendiente para este nodo')
            self.occupancy_reset_waiters[waiter['topic']] = waiter
        try:
            log.info('Ajuste de aforo solicitado: nodo=%s tópico=%s/command valor=%d', node_id, topic, value)
            info = self.client.publish(topic + '/command', occupancy_adjust_frame(value), qos=1, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('No se pudo enviar el ajuste de aforo')
            if not waiter['event'].wait(20):
                log.warning('Sin confirmación de ajuste de aforo: nodo=%s tópico esperado=%s', node_id, waiter['topic'])
                raise ValueError('El nodo no confirmó el ajuste de aforo; comprueba que esté conectado y tenga firmware actualizado')
            if waiter['result'] is not True:
                raise ValueError('El nodo no pudo ajustar el contador de aforo')
            log.info('Ajuste de aforo confirmado por nodo=%s valor=%d', node_id, value)
            return {'occupancy_adjusted': True, 'node_id': node_id, 'value': value}
        finally:
            with self.reset_lock:
                self.occupancy_reset_waiters.pop(waiter['topic'], None)

# --- Aplicación de ROI (roi.set + ACK) ---
    ROI_MODULE = 'shelf_cam'
    ROI_REQ_RE = re.compile(r'[A-Za-z0-9._:-]{1,64}')

    def set_rois(self, message):
        node_id = message.get('node_id')
        branch_id = message.get('branch_id')
        module = message.get('module') or self.ROI_MODULE
        req_id = message.get('req_id')
        version = message.get('version')
        rois = message.get('rois')
        if (not isinstance(node_id, str) or not UUID_RE.fullmatch(node_id)
                or not isinstance(branch_id, str) or not UUID_RE.fullmatch(branch_id)
                or module != self.ROI_MODULE):
            raise ValueError('Identidad del nodo o módulo inválido')
        if not isinstance(req_id, str) or not self.ROI_REQ_RE.fullmatch(req_id):
            raise ValueError('req_id inválido')
        if not isinstance(version, int) or isinstance(version, bool) or not 0 <= version <= 2**31 - 1:
            raise ValueError('Versión de zonas inválida')
        if not isinstance(rois, list):
            raise ValueError('Lista de zonas inválida')
        for roi in rois:
            if not isinstance(roi, dict) or not isinstance(roi.get('id'), str) or not roi['id']:
                raise ValueError('Zona inválida en la orden')
            for key in ('x', 'y', 'w', 'h'):
                value = roi.get(key)
                if (isinstance(value, bool) or not isinstance(value, (int, float))
                        or not math.isfinite(value)):
                    raise ValueError('Zona inválida en la orden')
        if not self.connected or not self.subscriptions_ready.wait(5):
            raise ValueError('MQTT no está listo para aplicar zonas')

        topic = f'selfiq/{branch_id}/{node_id}/{module}/command'
        waiter = {'event': threading.Event(), 'req_id': req_id, 'result': None}
        with self._roi_lock:
            if req_id in self._roi_waiters:
                raise ValueError('Ya hay una aplicación pendiente con ese req_id')
            self._roi_waiters[req_id] = waiter
        started = time.monotonic()
        try:
            payload = json.dumps({'type': 'roi.set', 'req_id': req_id,
                                  'version': version, 'rois': rois},
                                 separators=(',', ':'))
            log.info('ROI set solicitado: nodo=%s req=%s version=%s count=%d',
                     node_id, req_id, version, len(rois))
            info = self.client.publish(topic, payload, qos=1, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('No se pudo publicar roi.set')
            info.wait_for_publish(timeout=10)
            if not waiter['event'].wait(15):
                log.warning('ROI set sin ACK: nodo=%s req=%s timeout=15s', node_id, req_id)
                raise ValueError('Sin ACK del nodo (timeout 15s)')
            result = waiter['result']
            duration_ms = int((time.monotonic() - started) * 1000)
            if not result or result.get('ok') is not True:
                error = (result or {}).get('error') or 'El nodo rechazó las zonas'
                log.warning('ROI set rechazado: nodo=%s req=%s error=%s',
                            node_id, req_id, error)
                raise ValueError(error)
            ack_version = result.get('version')
            if not isinstance(ack_version, int) or isinstance(ack_version, bool):
                ack_version = version
            ack_count = result.get('count')
            if not isinstance(ack_count, int) or isinstance(ack_count, bool):
                ack_count = len(rois)
            log.info('ROI set confirmado: nodo=%s req=%s version=%s count=%s duracion_ms=%d',
                     node_id, req_id, ack_version, ack_count, duration_ms)
            return {'rois': True, 'ok': True, 'version': ack_version,
                    'count': ack_count, 'req_id': req_id}
        finally:
            with self._roi_lock:
                self._roi_waiters.pop(req_id, None)

    def notify_roi_ack(self, payload):
        if isinstance(payload, (bytes, bytearray, memoryview)):
            try:
                payload = json.loads(bytes(payload))
            except (TypeError, ValueError):
                return False
        if not isinstance(payload, dict) or payload.get('type') != 'roi_ack':
            return False
        req_id = payload.get('req_id')
        if not isinstance(req_id, str):
            return False
        with self._roi_lock:
            waiter = self._roi_waiters.get(req_id)
            if not waiter:
                return False
            waiter['result'] = {'ok': payload.get('ok') is True,
                                'version': payload.get('version'),
                                'count': payload.get('count'),
                                'error': (payload.get('error')
                                          if isinstance(payload.get('error'), str) else None)}
            waiter['event'].set()
            return True

    # --- Calibración de zonas (Fase 4) ---
    def _ensure_cal_store(self):
        with self._cal_store_lock:
            if self.cal_store is None:
                try:
                    self.cal_store = CalibrationStore(
                        calibration_store_path(self.config))
                except Exception:
                    log.exception('No se pudo abrir el almacén de calibración')
                    raise CalibrationCommandError(
                        'gateway_offline',
                        'Almacenamiento durable de calibración no disponible') from None
            return self.cal_store

    def run_calibration(self, command):
        """Valida, publica y espera el ``cal_result`` de una orden de calibración."""
        command = validate_command(command)
        store = self._ensure_cal_store()
        fingerprint = command_fingerprint(command)
        existing = store.operation(command['id'])
        if existing:
            if existing['command_hash'] != fingerprint:
                raise CalibrationCommandError(
                    'operation_conflict',
                    'El operation_id ya existe con otro cuerpo')
            if existing['result']:
                replay = json.loads(existing['result'])
                replay['req_id'] = command['req_id']
                log.info('Resultado durable reutilizado: operation=%s action=%s req=%s',
                         command['id'], command_action(command), command['req_id'])
                return replay
            raise CalibrationCommandError(
                'operation_conflict',
                'El resultado previo es desconocido; consulte calibration.status')
        store.begin_operation(command, fingerprint)

        if not self.connected or self.client is None:
            store.operation_status(command['id'], 'unknown')
            raise CalibrationCommandError('node_timeout', 'MQTT local no conectado')
        if not self.subscriptions_ready.wait(5):
            store.operation_status(command['id'], 'unknown')
            raise CalibrationCommandError(
                'node_timeout', 'MQTT no está listo para recibir el ACK')

        topic = (f"selfiq/{command['branch_id']}/{command['node_id']}"
                 f"/shelf_cam/command")
        waiter = {'event': threading.Event(), 'req_id': command['req_id'],
                  'command': command, 'ack': None, 'error': None}
        with self._cal_lock:
            if command['req_id'] in self._cal_waiters:
                raise CalibrationCommandError(
                    'busy', 'Ya hay una calibración pendiente con ese req_id')
            self._cal_waiters[command['req_id']] = waiter
        deadline = time.monotonic() + CALIBRATION_WAIT_SECONDS
        try:
            wire_command = {key: value for key, value in command.items()
                            if not key.startswith('_')}
            payload = json.dumps(wire_command, separators=(',', ':'))
            info = self.client.publish(topic, payload, qos=1, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                store.operation_status(command['id'], 'unknown')
                raise CalibrationCommandError(
                    'node_timeout', 'No se pudo publicar la orden de calibración')
            remaining = max(0, deadline - time.monotonic())
            info.wait_for_publish(timeout=min(2, remaining))
            store.operation_status(command['id'], 'sent')
            remaining = max(0, deadline - time.monotonic())
            if not waiter['event'].wait(remaining):
                store.operation_status(command['id'], 'unknown')
                if waiter['error']:
                    raise CalibrationCommandError('invalid_ack', waiter['error'])
                raise CalibrationCommandError(
                    'node_timeout', 'Sin ACK del nodo (timeout 10s)')
            ack = waiter['ack']
            store.complete_operation(command['id'], ack)
            log.info('Calibración confirmada: operation=%s action=%s req=%s',
                     command['id'], command_action(command), command['req_id'])
            return ack
        finally:
            with self._cal_lock:
                self._cal_waiters.pop(command['req_id'], None)

    def notify_cal_result(self, payload, branch_id, node_id, module):
        """Entrega un ``cal_result`` solo a su waiter y tras validarlo completo."""
        if module != 'shelf_cam' or not isinstance(payload, dict):
            return False
        if payload.get('type') != 'cal_result':
            return False
        req_id = payload.get('req_id')
        if not isinstance(req_id, str):
            return False
        with self._cal_lock:
            waiter = self._cal_waiters.get(req_id)
            if not waiter:
                return False
            command = waiter['command']
            if branch_id != command['branch_id'] or node_id != command['node_id']:
                log.warning('cal_result descartado: tópico no corresponde a la operación')
                return False
            try:
                validate_ack(command, payload)
            except CalibrationCommandError as exc:
                waiter['error'] = exc.detail
                log.warning('cal_result invalidado: operation=%s motivo=%s',
                            command['id'], exc.detail)
                return False
            waiter['ack'] = payload
            waiter['event'].set()
            return True

    def accept_calibration_reference(self, node_id, branch_id, meta, raw):
        """Correlaciona una referencia con su operación de captura y la encola."""
        store = self._ensure_cal_store()
        meta = parse_reference_meta(meta, raw)
        operation = store.capture_operation(meta['capture_id'])
        if not operation:
            raise CalibrationCommandError(
                'calibration_not_found', 'No hay una captura pendiente para esa imagen')
        expected = (
            ('branch_id', branch_id), ('node_id', node_id),
            ('calibration_id', meta['calibration_id']), ('roi_id', meta['roi_id']),
            ('kind', meta['kind']), ('context_hash', meta['context_hash']),
            ('calibration_version', meta['calibration_version']),
            ('format_version', meta['format_version']),
        )
        for field, value in expected:
            if operation.get(field) != value:
                raise CalibrationCommandError(
                    'operation_conflict',
                    f'La referencia no coincide con la captura ({field})')
        meta['_node_id'] = node_id
        meta['_branch_id'] = branch_id
        try:
            state, reference = store.put_reference(meta, raw)
        except ReferenceSpoolFull:
            raise CalibrationCommandError(
                'storage_full', 'Spool de referencias lleno; reintente más tarde') from None
        log.info('Referencia de calibración recibida: capture=%s estado=%s bytes=%d',
                 meta['capture_id'], state, len(raw))
        return state, reference

    def flush_calibration_reference(self, capture_id):
        store = self.cal_store
        if store is None:
            return
        reference = store.reference(capture_id)
        if reference and reference.get('image'):
            self._upload_reference(reference)

    def _upload_reference(self, reference):
        store = self.cal_store
        if store is None:
            return
        try:
            meta = json.loads(reference['meta'])
            raw = reference['image']
            if not raw:
                return
            result = self._post_calibration_reference(
                reference['node_id'], reference['branch_id'], meta, raw)
            if not isinstance(result, dict) or result.get('ok') is not True:
                raise ValueError('Respuesta de referencia inválida')
            store.mark_reference_uploaded(reference['capture_id'])
            log.info('Referencia reenviada al backend: capture=%s replay=%s',
                     reference['capture_id'], bool(result.get('replay')))
        except HTTPError as exc:
            status = exc.code
            detail = ''
            try:
                body = json.loads(exc.read(2048))
                if isinstance(body, dict):
                    detail = str(body.get('detail') or body.get('code') or '')[:200]
            except (ValueError, OSError):
                pass
            if status == 409:
                store.mark_reference_terminal(reference['capture_id'], 'conflict', detail or 'conflict')
            elif status in (408, 429) or status >= 500:
                delay = min(300, 5 * 2 ** min(int(reference.get('attempts') or 0), 6))
                store.mark_reference_retry(reference['capture_id'],
                                           f'HTTP {status} {detail}', delay)
            else:
                store.mark_reference_terminal(reference['capture_id'], 'failed',
                                              f'HTTP {status} {detail}')
        except (URLError, OSError, TimeoutError, ValueError, RuntimeError,
                CalibrationCommandError) as exc:
            delay = min(300, 5 * 2 ** min(int(reference.get('attempts') or 0), 6))
            store.mark_reference_retry(reference['capture_id'],
                                       f'{type(exc).__name__}: {exc}', delay)
            log.warning('Referencia %s se reintentará: %s',
                        reference['capture_id'], type(exc).__name__)

    def _post_calibration_reference(self, node_id, branch_id, meta, raw):
        if not self.backend.connected or self.backend.registration_status != 'registered':
            raise RuntimeError('Gateway sin autorización vigente')
        if self.authorize_node(branch_id, node_id) is None:
            raise PermissionError('Nodo no confirmado en el sistema')
        identity = self.backend.ensure_identity()
        timestamp = int(time.time())
        nonce = secrets.token_urlsafe(18)
        digest = hashlib.sha256(raw).hexdigest()
        data = (f'selfiq-calibration-reference-v1\\n{self.config.gateway_id}\\n'
                f'{timestamp}\\n{nonce}\\n{node_id}\\n{digest}').encode()
        signature = base64.b64encode(identity.key.sign(data)).decode()
        meta_b64 = base64.urlsafe_b64encode(
            json.dumps(meta, separators=(',', ':'), sort_keys=True).encode()
        ).decode().rstrip('=')
        url = self._api_url('/devices/' + quote(node_id, safe='')
                            + '/calibration-references')
        request = Request(url, data=raw, method='POST', headers={
            'Content-Type': 'image/jpeg',
            'X-Timestamp': str(timestamp),
            'X-Nonce': nonce,
            'X-Signature': signature,
            'X-Calibration-Meta': meta_b64,
        })
        with urlopen(request, timeout=15) as response:
            if response.status != 200:
                raise RuntimeError('Backend devolvió respuesta inesperada')
            return json.loads(response.read(4096))

    async def _calibration_uploads(self):
        while True:
            try:
                if self.backend.connected and self.cal_store is not None:
                    due = await asyncio.to_thread(self.cal_store.due_references, 4)
                    for reference in due:
                        await asyncio.to_thread(self._upload_reference, reference)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Fallo al reenviar referencias de calibración')
            await asyncio.sleep(5)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.connected = (reason_code == 0)
        if self.connected:
            self.subscriptions_ready.clear()
            result, self.subscription_mid = client.subscribe([(REQUEST, 1), ('selfiq/+/+/+/telemetry', 1),
                                                              ('selfiq/+/+/+/status', 1),
                                                              ('selfiq/+/+/+/command/ack', 1),
                                                              ('selfiq/+/+/+/capabilities', 1),
                                                              ('selfiq/+/+/control/result', 1)])
            if result != mqtt.MQTT_ERR_SUCCESS:
                log.error('No se pudo solicitar suscripción MQTT: rc=%s', result)
            else:
                log.info('MQTT conectado; solicitando suscripción a %s', REQUEST)
        else:
            log.warning('MQTT broker rechazó conexión: %s', reason_code)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        if not reason_codes or any(code.is_failure for code in reason_codes):
            log.error('Broker rechazó suscripción a %s: %s', REQUEST, reason_codes)
            return
        log.info('MQTT: Raspberry suscrita a %s', REQUEST)
        if mid == self.subscription_mid:
            self.subscriptions_ready.set()
            self._request_guide_states()

    def _on_connect_fail(self, client, userdata):
        log.warning('MQTT: no se pudo conectar a 127.0.0.1:1883; reintentando')

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False
        self.subscriptions_ready.clear()
        self._clear_guide_waiters()
        log.warning('MQTT broker desconectado: %s', reason_code)

    def _on_message(self, client, userdata, message):
        parts = message.topic.split('/')
        if len(parts) == 5 and parts[3:] == ['control', 'result']:
            if message.retain or len(message.payload) > 512:
                return
            try:
                result = parse_reset_result(message.payload)
                with self.reset_lock:
                    waiter = self.reset_waiters.get(message.topic)
                    if waiter and waiter['topic'] == message.topic:
                        waiter['result'] = result
                        waiter['event'].set()
                return
            except (ValueError, UnicodeError):
                pass
            try:
                result = parse_occupancy_adjust_result(message.payload)
                with self.reset_lock:
                    waiter = self.occupancy_reset_waiters.get(message.topic)
                    if waiter and waiter['topic'] == message.topic:
                        waiter['result'] = result
                        waiter['event'].set()
            except (ValueError, UnicodeError):
                pass
            return
        if len(parts) == 5 and parts[0] == 'selfiq' and parts[4] == 'status':
            if (message.retain or len(message.payload) > 512
                    or not UUID_RE.fullmatch(parts[1]) or not UUID_RE.fullmatch(parts[2])
                    or not re.fullmatch(r'[a-z0-9_]{1,32}', parts[3])
                    or parts[1] != (self.backend.branch_id or parts[1])):
                return
            if message.payload == b'online':
                self._queue_heartbeat(parts[2])
                return
            try:
                payload = json.loads(message.payload)
            except (TypeError, ValueError, UnicodeError):
                return
            if (isinstance(payload, dict)
                    and self._guide_record_sample(parts[1], parts[2], parts[3],
                                                  payload.get('boot'), payload.get('uptime_ms'))):
                self._queue_heartbeat(parts[2])
            return
        if len(parts) == 5 and parts[0] == 'selfiq' and parts[4] == 'capabilities':
            if (message.retain or len(message.payload) > 512
                    or not UUID_RE.fullmatch(parts[1]) or not UUID_RE.fullmatch(parts[2])
                    or not re.fullmatch(r'[a-z0-9_]{1,32}', parts[3])
                    or parts[1] != (self.backend.branch_id or parts[1])):
                return
            try:
                payload = json.loads(message.payload)
            except (TypeError, ValueError, UnicodeError):
                return
            self._receive_guide_capabilities(payload, parts[1], parts[2], parts[3])
            return
        if len(parts) == 6 and parts[0] == 'selfiq' and parts[4:] == ['command', 'ack']:
            if message.retain or len(message.payload) > 1024:
                log.warning('Command ACK descartado: retained o demasiado grande')
                return
            if (not UUID_RE.fullmatch(parts[1]) or not UUID_RE.fullmatch(parts[2])
                    or not re.fullmatch(r'[a-z0-9_]{1,32}', parts[3])):
                log.warning('Command ACK descartado: tópico inválido')
                return
            expected_branch = getattr(self.backend, 'branch_id', None)
            if expected_branch and parts[1] != expected_branch:
                log.warning('Command ACK descartado: sucursal distinta')
                return
            try:
                payload = json.loads(message.payload)
            except (TypeError, ValueError, UnicodeError):
                log.warning('Command ACK descartado: JSON inválido')
                return
            if not isinstance(payload, dict):
                return
            if payload.get('t') == 'guide.capabilities':
                # El firmware guía actual usa su publicador compartido
                # command/ack para este anuncio. No es un resultado ROI.
                if len(message.payload) <= 512:
                    self._receive_guide_capabilities(payload, parts[1], parts[2], parts[3])
                return
            if payload.get('t') in ('guide.result', 'guide.state'):
                self.notify_guide_result(payload, parts[1], parts[2], parts[3])
                return
            if payload.get('type') == 'cal_result':
                self.notify_cal_result(payload, parts[1], parts[2], parts[3])
            else:
                self.notify_roi_ack(payload)
            return
        if len(parts) == 5 and parts[0] == 'selfiq' and parts[4] == 'telemetry':
            if (not UUID_RE.fullmatch(parts[1]) or not UUID_RE.fullmatch(parts[2])
                    or not re.fullmatch(r'[a-z0-9_]{1,32}', parts[3]) or message.retain):
                log.warning('[STORAGE] Telemetria/topico invalido o retained; sin ACK')
                return
            # Recepción local solamente. El envío cloud exige autorización REST
            # vigente en influx_sync, también para eventos previos al reinicio.
            branch_id = parts[1]
            esp_id = parts[2]
            expected_branch = getattr(self.backend, 'branch_id', None)
            if expected_branch and branch_id != expected_branch:
                log.warning('[STORAGE] Telemetria de sucursal no registrada; sin ACK')
                return
            if self.outbox is None:
                log.error('[STORAGE] STORAGE_UNAVAILABLE; sin ACK')
                return
            try:
                event_id = self.outbox.put(message.topic, message.payload)
                self.storage_error = ''
                # Application receipt: device may release its RAM copy now.
                client.publish('/'.join(parts[:4]) + '/ack', event_id, qos=1, retain=False)
            except (ValueError, TypeError, UnicodeError) as exc:
                log.warning('[STORAGE] INVALID_EVENT: %s', type(exc).__name__)
            except Exception as exc:
                self.storage_error = 'STORAGE_FULL' if isinstance(exc, StorageFull) else 'STORAGE_ERROR'
                log.error('[STORAGE] %s; mensaje NO confirmado', self.storage_error)
            return
        if message.topic != REQUEST or len(message.payload) > 128:
            return
        try:
            raw = message.payload.decode('ascii').strip()
            # PETICIÓN ACTUAL: solo MAC. Para firmware de otros módulos se puede
            # extender a JSON {"mac":"...","node_type":"..."}.
            if raw.startswith('{'):
                value = json.loads(raw)
                mac = value['mac'].upper()
                node_type = value['node_type']
            else:
                mac, node_type = raw.upper(), self.default_node_type
            node_type = NODE_TYPE_ALIASES.get(node_type, node_type)
            if not MAC_RE.fullmatch(mac) or node_type not in NODE_TYPES:
                raise ValueError('Identificador incorrecto')
        except (ValueError, TypeError, KeyError, UnicodeDecodeError, AttributeError):
            log.warning('MQTT: solicitud de registro inválida')
            return

        def enqueue():
            if mac in self.pending or self.queue.full():
                return
            log.info('Solicitud ESP32 recibida: MAC=%s tipo=%s', mac, node_type)
            self.pending.add(mac)
            self.queue.put_nowait((mac, node_type))
        if self.loop is not None:
            self.loop.call_soon_threadsafe(enqueue)

    async def _worker(self):
        while True:
            mac, node_type = await self.queue.get()
            try:
                if not self.backend.connected or self.backend.registration_status != 'registered':
                    log.warning('Registro %s pendiente: el gateway no está registrado en Railway', mac)
                    continue  # La ESP32 reenvía cada 30 segundos hasta recibir un ID.
                response = await asyncio.to_thread(self._signed_post, mac, node_type)
                if not isinstance(response, dict) or response.get('registration_status') != 'registered':
                    raise ValueError('Backend no confirmó el registro')
                esp_id, branch_id = response['esp_id'], response['branch_id']
                if not UUID_RE.fullmatch(esp_id) or not UUID_RE.fullmatch(branch_id):
                    raise ValueError('El backend devolvió IDs incorrectos')
                client_id = 'esp-' + mac.replace(':', '').lower()
                topic = 'selfiq/esp32_setup/response/' + client_id
                # MAC | UUID de nodo | UUID de sucursal
                payload = f'{mac}|{esp_id}|{branch_id}'
                info = self.client.publish(topic, payload, qos=1, retain=False)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f'Publicación MQTT fallida rc={info.rc}')
                log.info('ESP32 registrada: %s -> %s (sucursal %s)', mac, esp_id, branch_id)
            except HTTPError as exc:
                detail = ''
                try:
                    body = json.loads(exc.read(2048))
                    if isinstance(body, dict) and isinstance(body.get('detail'), str):
                        detail = body['detail'][:300].replace('\n', ' ')
                except (ValueError, OSError):
                    pass
                log.error('Backend rechazó registro de %s (HTTP %s): %s', mac, exc.code, detail)
            except (URLError, OSError, TimeoutError, ValueError, KeyError, RuntimeError) as exc:
                log.warning('Registro de ESP32 pendiente (%s): %s', mac, type(exc).__name__)
            except Exception:
                log.exception('Error al registrar ESP32 %s; se permite reintento', mac)
            finally:
                self.pending.discard(mac)
                self.queue.task_done()

    async def start(self):
        log.info('Iniciando puente ESP32: MQTT local -> backend -> base de datos')
        self.loop = asyncio.get_running_loop()
        try:
            self.outbox = Outbox(self.config)
            log.info('[STORAGE] Cola SQLite lista; sincronizacion cloud pendiente de integrar')
        except Exception:
            self.storage_error = 'STORAGE_UNAVAILABLE'
            log.exception('[STORAGE] No se pudo abrir SQLite; red/registro siguen disponibles')
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id='selfiq-gateway-bridge')
        self.client.on_subscribe = self._on_subscribe
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.connect_async('127.0.0.1', 1883, keepalive=45)
        self.client.loop_start()
        self.worker = asyncio.create_task(self._worker(), name='selfiq-mqtt-registrations')
        self._ensure_cal_store()
        self._calibration_worker = asyncio.create_task(
            self._calibration_uploads(), name='selfiq-calibration-uploads')
        self._guide_capabilities_worker = asyncio.create_task(
            self._guide_capability_uploads(), name='selfiq-guide-capabilities')

    async def stop(self):
        if self._guide_capabilities_worker:
            self._guide_capabilities_worker.cancel()
            await asyncio.gather(self._guide_capabilities_worker, return_exceptions=True)
            self._guide_capabilities_worker = None
        if self._calibration_worker:
            self._calibration_worker.cancel()
            await asyncio.gather(self._calibration_worker, return_exceptions=True)
            self._calibration_worker = None
        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        if self.client:
            self.client.disconnect()
            await asyncio.to_thread(self.client.loop_stop)

        if self.outbox:
            await asyncio.to_thread(self.outbox.close)
            self.outbox = None
        if self.cal_store:
            await asyncio.to_thread(self.cal_store.close)
            self.cal_store = None
