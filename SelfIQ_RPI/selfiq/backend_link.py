"""Outbound command channel. Wi-Fi credentials never travel through this link."""
import asyncio
from collections import OrderedDict
import json
import logging
import random
from urllib.parse import quote

from websockets.asyncio.client import connect
from .backend_config import websocket_url
from .gateway_identity import GatewayIdentity, validate_backend

log = logging.getLogger('selfiq.backend')


class BackendLink:
    def __init__(self, config, execute):
        self.config, self.execute = config, execute
        self.connected = False
        self.results = OrderedDict()
        self.identity = None
        self.registration_status = 'not_configured'
        self.branch_id = None
        self.changed = asyncio.Event()
        self.socket = None

    async def process(self, raw):
        try:
            message = json.loads(raw)
        except (ValueError, TypeError):
            return {'type': 'protocol_error', 'error': 'JSON inválido'}
        if not isinstance(message, dict):
            return {'type': 'protocol_error', 'error': 'Se requiere un objeto'}
        identifier, command = message.get('id'), message.get('command')
        if message.get('type') != 'command' or not isinstance(identifier, str) or not 1 <= len(identifier) <= 64:
            return {'type': 'protocol_error', 'error': 'Orden inválida'}
        # A repeated ID must not reopen AP or reset its inactivity timer.
        if identifier in self.results:
            old_command, response = self.results[identifier]
            if command != old_command:
                return {'type': 'result', 'id': identifier, 'ok': False, 'error': 'ID reutilizado para otra orden'}
            return response
        response = {'type': 'result', 'id': identifier}
        try:
            if command not in ('tcp.enable', 'tcp.disable', 'status'):
                raise ValueError('Orden no permitida')
            operation = asyncio.create_task(self.execute(command))
            try:
                data = await asyncio.shield(operation)
            except asyncio.CancelledError:
                # to_thread cannot be stopped: finish before service shutdown.
                await operation
                raise
            response.update(ok=True, data=data)
        except ValueError as exc:
            response.update(ok=False, error=str(exc))
        except Exception:
            log.exception('Fallo al ejecutar orden del backend')
            response.update(ok=False, error='Fallo al ejecutar; consulte logs de la Raspberry')
        self.results[identifier] = (command, response)
        if len(self.results) > 128:
            self.results.popitem(last=False)
        return response

    def ensure_identity(self):
        if self.identity is None:
            self.identity = GatewayIdentity(self.config)
        return self.identity

    async def configure(self, url, token):
        validate_backend(url, token)
        identity = self.ensure_identity()
        await asyncio.to_thread(identity.save, url, token)
        self.registration_status = 'connecting'
        self.changed.set()
        if self.socket is not None:
            await self.socket.close(code=1012)

    async def run(self):
        identity = self.ensure_identity()
        delay = 1
        while True:
            self.changed.clear()
            if not self.config.backend_ws_url or not self.config.gateway_token:
                self.registration_status = 'not_configured'
                await self.changed.wait()
                continue
            url = websocket_url(self.config.backend_ws_url) + '/' + quote(self.config.gateway_id, safe='')
            try:
                log.info('Conectando gateway %s al backend %s', self.config.gateway_id,
                         'por WSS/TLS' if url.startswith('wss://') else 'de desarrollo local')
                async with connect(url, ping_interval=20, ping_timeout=20, close_timeout=5,
                                   open_timeout=10, max_size=8192, max_queue=8) as ws:
                    self.socket = ws
                    challenge = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    if not isinstance(challenge, dict) or challenge.get('type') != 'challenge':
                        raise ValueError('Protocolo de registro incompatible')
                    await ws.send(json.dumps(identity.proof(challenge.get('nonce'))))
                    auth = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    if not isinstance(auth, dict) or auth.get('type') != 'auth_status':
                        raise ValueError('Respuesta de registro incorrecta')
                    self.registration_status = auth.get('status', 'registration_rejected')
                    self.branch_id = auth.get('branch_id')
                    self.connected = self.registration_status == 'registered'
                    delay = 1
                    log.info('Registro backend: %s', self.registration_status)
                    if self.registration_status not in ('registered', 'token_invalid'):
                        await ws.wait_closed()
                    else:
                        async for raw in ws:
                            message = json.loads(raw)
                            if not isinstance(message, dict):
                                raise ValueError('Mensaje backend incorrecto')
                            if message.get('type') == 'auth_status':
                                self.registration_status = message.get('status', 'backend_unavailable')
                                self.connected = self.registration_status == 'registered'
                                continue
                            if message.get('type') == 'command' and message.get('command') == 'gateway.token':
                                # Available only on the authenticated device-identity channel.
                                try:
                                    await asyncio.to_thread(identity.save, self.config.backend_ws_url, message.get('branch_token'))
                                    response = {'type':'result','id':message.get('id'),'ok':True,
                                                'data':{'token_saved':True}}
                                except (ValueError, TypeError, OSError):
                                    response = {'type':'result','id':message.get('id'),'ok':False,
                                                'error':'No pudo guardarse el token'}
                                await ws.send(json.dumps(response))
                                if response['ok']:
                                    self.registration_status = 'connecting'
                                    self.changed.set()
                                    await ws.close(code=1012)
                                    break
                            elif self.connected:
                                await ws.send(json.dumps(await self.process(raw)))
                            else:
                                await ws.send(json.dumps({'type':'result','id':message.get('id'),'ok':False,
                                                          'error':'Token inválido; solo permite corregir registro'}))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.registration_status not in ('token_invalid', 'registration_rejected'):
                    self.registration_status = 'backend_unavailable'
                log.warning('Backend desconectado (%s); reintentando', type(exc).__name__)
            finally:
                self.connected = False
                self.socket = None
            if not self.changed.is_set():
                try:
                    await asyncio.wait_for(self.changed.wait(), delay + random.uniform(0, delay / 4))
                except asyncio.TimeoutError:
                    pass
            delay = min(delay * 2, 30)