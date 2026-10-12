import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

import httpx
import uvicorn
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus
from dev.backend import create_app as create_backend
from selfiq.app import create_app
from selfiq.backend_link import BackendLink
from selfiq.models import Config, Credentials, atomic_json
from selfiq.protocol import HEADER, encode, read_frame, credentials_frame
from test_selfiq import FakeNetwork


@asynccontextmanager
async def serve(app):
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='error', access_log=False))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        for _ in range(200):
            if server.started:
                break
            if task.done():
                await task
                raise RuntimeError('Servidor no inició')
            await asyncio.sleep(.01)
        else:
            raise RuntimeError('Timeout de inicio')
        yield port
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        sock.close()


class MessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_id_does_not_execute_twice_and_reuse_rejected(self):
        calls = []
        async def execute(command):
            calls.append(command)
            return {'tcp_activo': True}
        link = BackendLink(Config(), execute)
        message = json.dumps({'type': 'command', 'id': 'one', 'command': 'tcp.enable'})
        first = await link.process(message)
        self.assertTrue(first['ok'])
        self.assertEqual(await link.process(message), first)
        self.assertEqual(calls, ['tcp.enable'])
        response = await link.process(json.dumps({'type': 'command', 'id': 'one', 'command': 'tcp.disable'}))
        self.assertFalse(response['ok'])

    async def test_malformed_unknown_and_execution_failure(self):
        async def execute(command): raise ValueError('Wi-Fi no conectado')
        link = BackendLink(Config(), execute)
        for raw in ('{', '[]', '{}'):
            self.assertEqual((await link.process(raw))['type'], 'protocol_error')
        for command in ('reset.credentials', 'tcp.enable'):
            response = await link.process(json.dumps({'type':'command', 'id':command, 'command':command}))
            self.assertFalse(response['ok'])

    async def test_cancel_waits_for_inflight_mutation(self):
        started, finished = asyncio.Event(), asyncio.Event()
        async def execute(command):
            started.set()
            await finished.wait()
            return {}
        link = BackendLink(Config(), execute)
        task = asyncio.create_task(link.process('{"type":"command","id":"one","command":"tcp.enable"}'))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        finished.set()
        with self.assertRaises(asyncio.CancelledError): await task


class ConfigurationTests(unittest.TestCase):
    def test_old_config_and_environment_override(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps({'admin_token': 'A'*32}))
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(Config.load(path).backend_ws_url, '')
            values = {'SELFIQ_BACKEND_WS_URL': 'ws://192.168.1.100:8000/ws/gateways',
                      'SELFIQ_GATEWAY_ID': 'rpi-01', 'SELFIQ_GATEWAY_TOKEN': 'G'*32}
            with patch.dict(os.environ, values, clear=True):
                self.assertEqual(Config.load(path).gateway_id, 'rpi-01')
            values['SELFIQ_GATEWAY_ID'] = '../another-gateway'
            with patch.dict(os.environ, values, clear=True):
                with self.assertRaises(ValueError): Config.load(path)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_backend_to_rpi_to_esp_real_sockets_reconnect_and_credentials_unchanged(self):
        backend = create_backend({'rpi-01':'G'*32}, 'A'*32)
        with tempfile.TemporaryDirectory() as directory:
            config = replace(Config(), admin_token='L'*32, state_file=directory+'/credentials.json',
                             ap_ip='127.0.0.1', tcp_port=0, gateway_id='rpi-01', gateway_token='G'*32)
            atomic_json(config.state_file, {'profile':'selfiq-sta-a',
                        'credentials': {'ssid':'Local','password':'12345678','kind':1,'username':''}})
            saved = Path(config.state_file).read_bytes()
            async with serve(backend) as port:
                config.backend_ws_url = f'ws://127.0.0.1:{port}/ws/gateways'
                rpi = create_app(config, FakeNetwork())
                async with serve(rpi):
                    async with httpx.AsyncClient(trust_env=False, base_url=f'http://127.0.0.1:{port}',
                                                 headers={'X-SelfIQ-Token':'A'*32}) as client:
                        for _ in range(200):
                            if 'rpi-01' in backend.state.sessions: break
                            await asyncio.sleep(.01)
                        self.assertIn('rpi-01', backend.state.sessions)
                        response = await client.post('/gateways/rpi-01/commands', json={'command':'tcp.enable'})
                        result = response.json()
                        self.assertTrue(result['ok'])
                        self.assertTrue(result['data']['tcp_activo'])
                        self.assertNotIn('ssid', result['data'])
                        self.assertNotIn('12345678', json.dumps(result))
                        tcp = rpi.state.tcp
                        reader, writer = await asyncio.open_connection('127.0.0.1', tcp.server.sockets[0].getsockname()[1])
                        writer.write(encode(HEADER, 0, 1, b'\x00'))
                        await writer.drain()
                        self.assertEqual((await read_frame(reader)).data, credentials_frame(Credentials('Local','12345678'))[4:])
                        stopped = (await client.post('/gateways/rpi-01/commands', json={'command':'tcp.disable'})).json()
                        self.assertTrue(stopped['ok'])
                        self.assertFalse(stopped['data']['tcp_activo'])
                        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b'')
                        writer.close()
                        await writer.wait_closed()
                        self.assertEqual(Path(config.state_file).read_bytes(), saved)
                        self.assertTrue(rpi.state.controller.online)
                        old = backend.state.sessions['rpi-01']
                        await old['ws'].close(code=1012)
                        for _ in range(400):
                            if backend.state.sessions.get('rpi-01') not in (None, old): break
                            await asyncio.sleep(.01)
                        self.assertIsNot(backend.state.sessions.get('rpi-01'), old)
                        self.assertIn('rpi-01', backend.state.sessions)
                        result = (await client.post('/gateways/rpi-01/commands', json={'command':'status'})).json()
                        self.assertTrue(result['ok'])

    async def test_auth_offline_unknown_and_invalid_command(self):
        backend = create_backend({'rpi-01':'G'*32}, 'A'*32)
        async with serve(backend) as port:
            async with httpx.AsyncClient(trust_env=False, base_url=f'http://127.0.0.1:{port}') as client:
                path = '/gateways/rpi-01/commands'
                self.assertEqual((await client.post(path, json={'command':'tcp.enable'})).status_code, 403)
                client.headers['X-SelfIQ-Token'] = 'A'*32
                self.assertEqual((await client.post(path, json={'command':'tcp.enable'})).status_code, 503)
                self.assertEqual((await client.post(path, json={'command':'wipe'})).status_code, 422)
                self.assertEqual((await client.get('/gateways/other')).status_code, 404)
            with self.assertRaises(InvalidStatus):
                async with connect(f'ws://127.0.0.1:{port}/ws/gateways/rpi-01',
                                   additional_headers={'Authorization':'Bearer wrong'}): pass

    async def test_timeout_and_disconnect_do_not_report_success(self):
        backend = create_backend({'rpi-01':'G'*32}, 'A'*32, timeout=.05)
        async with serve(backend) as port:
            async with connect(f'ws://127.0.0.1:{port}/ws/gateways/rpi-01',
                               additional_headers={'Authorization':'Bearer '+'G'*32}) as ws:
                async with httpx.AsyncClient(trust_env=False, base_url=f'http://127.0.0.1:{port}',
                                             headers={'X-SelfIQ-Token':'A'*32}) as client:
                    task = asyncio.create_task(client.post('/gateways/rpi-01/commands', json={'command':'status'}))
                    await ws.recv()
                    self.assertEqual((await task).status_code, 504)
                    task = asyncio.create_task(client.post('/gateways/rpi-01/commands', json={'command':'status'}))
                    await ws.recv()
                    await ws.close()
                    self.assertEqual((await task).status_code, 503)