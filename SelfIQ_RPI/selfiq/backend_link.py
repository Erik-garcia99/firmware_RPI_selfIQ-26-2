"""Outbound command channel. Wi-Fi credentials never travel through this link."""
import asyncio
from collections import OrderedDict
import json
import logging
import random
from urllib.parse import quote

from websockets.asyncio.client import connect

log = logging.getLogger('selfiq.backend')


class BackendLink:
    def __init__(self, config, execute):
        self.config, self.execute = config, execute
        self.connected = False
        self.results = OrderedDict()

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

    async def run(self):
        if not self.config.backend_ws_url:
            return
        url = self.config.backend_ws_url.rstrip('/') + '/' + quote(self.config.gateway_id, safe='')
        delay = 1
        while True:
            try:
                async with connect(url, additional_headers={'Authorization': 'Bearer ' + self.config.gateway_token},
                                   ping_interval=20, ping_timeout=20, close_timeout=5,
                                   open_timeout=10, max_size=8192, max_queue=8) as ws:
                    self.connected = True
                    delay = 1
                    log.info('Conexión con backend establecida')
                    async for raw in ws:
                        await ws.send(json.dumps(await self.process(raw)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Do not log URLs, tokens, frames or Wi-Fi credentials.
                log.warning('Backend desconectado (%s); reintentando', type(exc).__name__)
            finally:
                self.connected = False
            await asyncio.sleep(delay + random.uniform(0, delay / 4))
            delay = min(delay * 2, 30)