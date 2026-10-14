import asyncio
import re
from dataclasses import replace
import unittest
import tempfile
from pathlib import Path
import httpx
from selfiq.app import create_app, TCPService
from selfiq.models import Config, Credentials
from selfiq.controller import Controller
from selfiq.protocol import *
from test_selfiq import FakeNetwork

class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_reopen_reuses_credentials_without_web_and_closes_sockets(self):
        with tempfile.TemporaryDirectory() as d:
            c = replace(Config(), state_file=d + '/credentials.json', admin_token='T'*32,
                        ap_ip='127.0.0.1', tcp_port=0)
            net = FakeNetwork()
            app = create_app(c, net)
            ctrl, tcp = app.state.controller, app.state.tcp
            ctrl.configure(Credentials('Local', '12345678'))
            original = Path(c.state_file).read_bytes()
            ctrl.stop_provisioning()
            client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('10.1.1.20', 100)),
                                       base_url='http://10.1.1.1')
            headers = {'X-SelfIQ-Token': c.admin_token}
            try:
                self.assertEqual((await client.post('/admin/tcp/enable')).status_code, 403)
                response = await client.post('/admin/tcp/enable', headers=headers)
                self.assertEqual(response.status_code, 202)
                for _ in range(100):
                    if not ctrl.busy and tcp.server is not None:
                        break
                    await asyncio.sleep(.01)
                self.assertIsNotNone(tcp.server)
                state = (await client.get('/status', headers=headers)).json()
                self.assertTrue(state['tcp_activo'])
                self.assertFalse(state['configuracion_requerida'])
                self.assertEqual((await client.get('/', headers=headers)).status_code, 409)
                reader, writer = await asyncio.open_connection('127.0.0.1', tcp.server.sockets[0].getsockname()[1])
                writer.write(encode(HEADER, 0, 1, b'\x00'))
                await writer.drain()
                self.assertEqual((await read_frame(reader)).data, credentials_frame(ctrl.creds)[4:])
                self.assertEqual(Path(c.state_file).read_bytes(), original)
                self.assertEqual(net.current, ctrl.profile)
                self.assertEqual((await client.post('/admin/tcp/disable', headers=headers)).status_code, 200)
                self.assertEqual(await asyncio.wait_for(reader.read(), 1), b'')
                writer.close()
                await writer.wait_closed()
                self.assertTrue(ctrl.online)
                self.assertEqual(Path(c.state_file).read_bytes(), original)
                # A genuine failure brings configuration back without erasing state.
                net.current, net.accept = None, False
                ctrl.tick()
                self.assertEqual((await client.get('/', headers=headers)).status_code, 200)
                self.assertEqual((await client.post('/admin/tcp/enable', headers=headers)).status_code, 409)
                self.assertEqual(Path(c.state_file).read_bytes(), original)
            finally:
                await tcp.close()
                await client.aclose()

    async def test_configuration_submission_blocked_while_wifi_connected(self):
        app = create_app(replace(Config(), admin_token='T'*32), FakeNetwork())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1') as client:
            page = await client.get('/')
            token = re.search(r"const session\s*=\s*'([^']+)'", page.text).group(1)
            app.state.controller.online = True
            r = await client.post('/configure', headers={'X-SelfIQ-Session': token},
                                  json={'ssid': 'Otro', 'password': 'abcdefgh'})
            self.assertEqual(r.status_code, 409)

    async def test_http_page_status_invalid_input_and_csrf(self):
        c = replace(Config(), admin_token='T'*32)
        net = FakeNetwork()
        app = create_app(c, net)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 100)), base_url='http://127.0.0.1') as client:
            r = await client.get('/')
            self.assertEqual(r.status_code, 200)
            self.assertIn('192.168.50.1', r.text)
            self.assertEqual(r.headers['cache-control'], 'no-store')
            token = re.search(r"const session\s*=\s*'([^']+)'", r.text).group(1)
            self.assertEqual((await client.get('/status')).json()['estado'], 'esperando_configuracion')
            r = await client.post('/configure', json={'ssid': 'Casa', 'password': '12345678'})
            self.assertEqual(r.status_code, 403)
            r = await client.post('/configure', headers={'X-SelfIQ-Session': token}, json={'ssid':'Casa', 'password':'bad'})
            self.assertEqual(r.status_code, 422)
            r = await client.post('/configure', headers={'X-SelfIQ-Session': token, 'Origin':'http://malicious.example'}, json={})
            self.assertEqual(r.status_code, 403)
            r = await client.post('/configure', headers={'X-SelfIQ-Session':token}, content='A'*2049)
            self.assertEqual(r.status_code, 413)
            self.assertEqual((await client.post('/admin/ap/enable')).status_code, 403)
            app.state.controller.busy = True
            r = await client.post('/configure', headers={'X-SelfIQ-Session':token}, json={})
            self.assertEqual(r.status_code, 409)
    async def test_lan_access_restricted(self):
        app = create_app(replace(Config(), admin_token='T'*32), FakeNetwork())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('10.1.1.20', 100)), base_url='http://10.1.1.1') as client:
            self.assertEqual((await client.get('/')).status_code, 403)
            self.assertEqual((await client.get('/status', headers={'X-SelfIQ-Token':'T'*32})).status_code, 200)

class TCPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.net = FakeNetwork()
        self.ctrl = Controller(replace(Config(), ap_ip='127.0.0.1', tcp_port=0), self.net)
        self.ctrl.creds = Credentials('Casa', '12345678')
        self.ctrl.online = True
        self.net.ap_active = True
        self.service = TCPService(self.ctrl)
        await self.service.reconcile()
        self.port = self.service.server.sockets[0].getsockname()[1]
    async def asyncTearDown(self):
        await self.service.close()
        await asyncio.sleep(0)
    async def test_real_socket_fragmentation_and_ack(self):
        r, w = await asyncio.open_connection('127.0.0.1', self.port)
        packet = encode(HEADER, 0, 1, b'\x00')
        for b in packet:
            w.write(bytes([b]))
            await w.drain()
            await asyncio.sleep(.001)
        response = await read_frame(r)
        self.assertEqual(response, Frame(ACK, 1, 2, credentials_frame(self.ctrl.creds)[4:]))
        self.assertEqual(self.ctrl.delivered, 1)
        w.write(encode(ACK, 3, 0, b'\x01'))
        await w.drain()
        self.assertEqual(await asyncio.wait_for(r.read(), 1), b'')
        w.close()
        await w.wait_closed()
    async def test_nack_before_validation_and_bad_request_not_touch_timer(self):
        r, w = await asyncio.open_connection('127.0.0.1', self.port)
        original = self.ctrl.last_request
        self.ctrl.online = False
        w.write(encode(HEADER, 0, 1, b'\x00'))
        await w.drain()
        self.assertTrue((await read_frame(r)).nack)
        self.ctrl.online = True
        w.write(encode(HEADER, 0, 7, b'\x00'))
        await w.drain()
        self.assertTrue((await read_frame(r)).nack)
        self.assertEqual(self.ctrl.last_request, original)
        w.close()
        await w.wait_closed()
    async def test_listener_and_existing_clients_close_with_ap(self):
        r, w = await asyncio.open_connection('127.0.0.1', self.port)
        await asyncio.sleep(0)
        self.net.ap_active = False
        await self.service.reconcile()
        self.assertIsNone(self.service.server)
        self.assertEqual(await asyncio.wait_for(r.read(), 1), b'')
        with self.assertRaises(OSError):
            await asyncio.open_connection('127.0.0.1', self.port)
        w.close()
        await w.wait_closed()

    async def test_idle_timeout_closes_listener_and_clients_after_valid_request(self):
        now = [1000]
        self.ctrl.clock = lambda: now[0]
        self.ctrl.profile = 'selfiq-sta-a'
        self.net.current = self.ctrl.profile
        self.ctrl.touch()
        reader, writer = await asyncio.open_connection('127.0.0.1', self.port)
        now[0] += 299
        writer.write(encode(HEADER, 0, 1, b'\x00'))
        await writer.drain()
        await read_frame(reader)
        now[0] += 299
        self.ctrl.tick()
        await self.service.reconcile()
        self.assertIsNotNone(self.service.server)
        now[0] += 1
        self.ctrl.tick()
        await self.service.reconcile()
        self.assertIsNone(self.service.server)
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b'')
        self.assertTrue(self.ctrl.online)
        writer.close()
        await writer.wait_closed()