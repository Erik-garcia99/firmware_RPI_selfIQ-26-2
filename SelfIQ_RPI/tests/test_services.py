import asyncio
from dataclasses import replace
import unittest
import httpx
from selfiq.app import create_app, TCPService
from selfiq.models import Config, Credentials
from selfiq.controller import Controller
from selfiq.protocol import *
from test_selfiq import FakeNetwork

class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_page_status_invalid_input_and_csrf(self):
        c = replace(Config(), admin_token='T'*32)
        net = FakeNetwork()
        app = create_app(c, net)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 100)), base_url='http://127.0.0.1') as client:
            r = await client.get('/')
            self.assertEqual(r.status_code, 200)
            self.assertIn('192.168.50.1', r.text)
            self.assertEqual(r.headers['cache-control'], 'no-store')
            token = r.text.split("const session='")[1].split("'")[0]
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
