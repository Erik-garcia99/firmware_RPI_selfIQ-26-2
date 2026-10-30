"""MQTT local -> alta REST firmada -> respuesta MQTT individual (sin tocar STA/AP)."""
import asyncio
import base64
import json
import logging
import re
import secrets
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import paho.mqtt.client as mqtt

from .backend_config import websocket_url
from .outbox import Outbox, StorageFull

log = logging.getLogger('selfiq.mqtt')
MAC_RE = re.compile(r'^(?:[0-9A-F]{2}:){5}[0-9A-F]{2}$')
UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
REQUEST = 'selfiq/esp32_setup'
NODE_TYPES = {'guide', 'access_counter', 'shelf_cam', 'environment'}
NODE_TYPE_ALIASES = {'shelf_guide': 'guide', 'occupancy': 'access_counter', 'camera': 'shelf_cam'}


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
        self.default_node_type = 'guide'  # Firmware actual: shelf guide; configurar para otros tipos.

    def endpoint(self):
        base = urlsplit(websocket_url(self.config.backend_ws_url))
        scheme = 'https' if base.scheme == 'wss' else 'http'
        return urlunsplit((scheme, base.netloc,
                           base.path.removesuffix('/ws/gateways') + '/gateways/'
                           + quote(self.config.gateway_id, safe='') + '/nodes/register', '', ''))

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

    def test_light(self, message):
        """Publica la prueba en el tópico de comandos del nodo (sin GPIO aún)."""
        node_id = message.get('node_id')
        branch_id = message.get('branch_id')
        module = message.get('module') or 'shelf_monitor'
        channel = message.get('channel')
        duration_ms = message.get('duration_ms', 5000)
        if (not isinstance(node_id, str) or not UUID_RE.fullmatch(node_id)
                or not isinstance(branch_id, str) or not UUID_RE.fullmatch(branch_id)
                or not isinstance(module, str) or not re.fullmatch(r'[a-z0-9_]{1,32}', module)
                or not isinstance(channel, int) or isinstance(channel, bool) or not 0 <= channel < 10
                or not isinstance(duration_ms, int) or isinstance(duration_ms, bool)
                or not 100 <= duration_ms <= 30000):
            raise ValueError('Orden de prueba de luz inválida')
        if not self.connected or self.client is None:
            raise ValueError('MQTT local no conectado')
        topic = f'selfiq/{branch_id}/{node_id}/{module}/command'
        payload = json.dumps({'type': 'light.test', 'channel': channel, 'duration_ms': duration_ms})
        info = self.client.publish(topic, payload, qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f'Publicación MQTT fallida rc={info.rc}')
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise RuntimeError('El broker no confirmó la publicación MQTT')
        log.info('Prueba de luz publicada en %s (canal %s)', topic, channel)
        return {'published': True, 'channel': channel, 'duration_ms': duration_ms}

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.connected = (reason_code == 0)
        if self.connected:
            result, _ = client.subscribe([(REQUEST, 1), ('selfiq/+/+/+/telemetry', 1)])
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

    def _on_connect_fail(self, client, userdata):
        log.warning('MQTT: no se pudo conectar a 127.0.0.1:1883; reintentando')

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False
        log.warning('MQTT broker desconectado: %s', reason_code)

    def _on_message(self, client, userdata, message):
        parts = message.topic.split('/')
        if len(parts) == 5 and parts[0] == 'selfiq' and parts[4] == 'telemetry':
            if (not UUID_RE.fullmatch(parts[1]) or not UUID_RE.fullmatch(parts[2])
                    or not re.fullmatch(r'[a-z0-9_]{1,32}', parts[3]) or message.retain):
                log.warning('[STORAGE] Telemetria/topico invalido o retained; sin ACK')
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

    async def stop(self):
        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        if self.client:
            self.client.disconnect()
            await asyncio.to_thread(self.client.loop_stop)

        if self.outbox:
            await asyncio.to_thread(self.outbox.close)
            self.outbox = None
