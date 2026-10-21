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

from .gateway_identity import GatewayIdentity

log = logging.getLogger('selfiq.mqtt')
MAC_RE = re.compile(r'^(?:[0-9A-F]{2}:){5}[0-9A-F]{2}$')
UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
REQUEST = 'selfiq/esp32_setup'


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
        self.default_node_type = 'guide'  # Firmware actual: shelf guide; configurar para otros tipos.

    def endpoint(self):
        base = urlsplit(self.config.backend_ws_url)
        scheme = 'https' if base.scheme == 'wss' else 'http'
        return urlunsplit((scheme, base.netloc,
                           '/gateways/' + quote(self.config.gateway_id, safe='') + '/nodes/register', '', ''))

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

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.connected = (reason_code == 0)
        if self.connected:
            client.subscribe(REQUEST, qos=1)
            log.info('MQTT: Raspberry suscrita a %s', REQUEST)
        else:
            log.warning('MQTT broker rechazó conexión: %s', reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False
        log.warning('MQTT broker desconectado: %s', reason_code)

    def _on_message(self, client, userdata, message):
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
            if not MAC_RE.fullmatch(mac) or node_type not in ('guide', 'access_counter', 'shelf_cam', 'environment'):
                raise ValueError('Identificador incorrecto')
        except (ValueError, TypeError, KeyError, UnicodeDecodeError, AttributeError):
            log.warning('MQTT: solicitud de registro inválida')
            return

        def enqueue():
            if mac in self.pending or self.queue.full():
                return
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
                log.error('Backend rechazó registro de %s (HTTP %s)', mac, exc.code)
            except (URLError, OSError, TimeoutError, ValueError, KeyError, RuntimeError) as exc:
                log.warning('Registro de ESP32 pendiente (%s): %s', mac, type(exc).__name__)
            finally:
                self.pending.discard(mac)
                self.queue.task_done()

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id='selfiq-gateway-bridge')
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
