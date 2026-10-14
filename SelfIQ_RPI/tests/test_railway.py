"""Cloud configuration and the signed gateway protocol; no hardware/DB required."""
import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.x509.oid import NameOID
from websockets.asyncio.client import connect

from selfiq.backend_config import websocket_url
from selfiq.backend_link import BackendLink
from selfiq.gateway_identity import GatewayIdentity
from selfiq.models import Config


def signed_backend(gateways, api_token):
    """Test peer implementing the deployed backend's challenge/enroll exchange."""
    app = FastAPI()
    app.state.sessions = {}
    app.state.proofs = []

    @app.websocket('/ws/gateways/{identifier}')
    async def link(ws: WebSocket, identifier: str):
        await ws.accept()
        nonce = secrets.token_urlsafe(32)
        await ws.send_json({'type': 'challenge', 'nonce': nonce})
        proof = await ws.receive_json()
        key = ed25519.Ed25519PublicKey.from_public_bytes(base64.b64decode(proof['public_key']))
        key.verify(base64.b64decode(proof['signature']),
                   ('selfiq-gateway-v1:' + identifier + ':' + nonce).encode())
        app.state.proofs.append(proof)
        if proof['branch_token'] != gateways.get(identifier):
            await ws.send_json({'type': 'auth_status', 'status': 'registration_rejected'})
            await ws.close(code=1008)
            return
        session = {'ws': ws, 'pending': {}}
        app.state.sessions[identifier] = session
        await ws.send_json({'type': 'auth_status', 'status': 'registered', 'branch_id': 'branch-01'})
        try:
            while True:
                result = await ws.receive_json()
                future = session['pending'].get(result.get('id'))
                if future and not future.done():
                    future.set_result(result)
        except WebSocketDisconnect:
            pass
        finally:
            if app.state.sessions.get(identifier) is session:
                app.state.sessions.pop(identifier, None)

    @app.post('/gateways/{identifier}/commands')
    async def command(identifier: str, request: Request):
        if request.headers.get('X-SelfIQ-Token') != api_token:
            raise HTTPException(403)
        session = app.state.sessions.get(identifier)
        if not session:
            raise HTTPException(503)
        data = await request.json()
        message_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        session['pending'][message_id] = future
        await session['ws'].send_json({'type': 'command', 'id': message_id, **data})
        try:
            return await asyncio.wait_for(future, 3)
        finally:
            session['pending'].pop(message_id, None)
    return app


class CloudConfigTests(unittest.TestCase):
    def test_url_conversion(self):
        for value in ('https://selfiq.up.railway.app', 'https://selfiq.up.railway.app/',
                      'wss://selfiq.up.railway.app/ws/gateways/',
                      'https://selfiq.up.railway.app/ws/gateways'):
            self.assertEqual(websocket_url(value), 'wss://selfiq.up.railway.app/ws/gateways')
        self.assertEqual(websocket_url('https://example.com/api'), 'wss://example.com/api/ws/gateways')
        self.assertEqual(websocket_url('ws://127.0.0.1:8000/ws/gateways'),
                         'ws://127.0.0.1:8000/ws/gateways')

    def test_reject_insecure_public_private_railway_and_credentials(self):
        for value in ('http://selfiq.up.railway.app', 'ws://selfiq.up.railway.app',
                      'https://backend.railway.internal', 'https://user:pass@example.com',
                      'https://example.com?token=secret', 'https://example.com#token',
                      'https://example.com:bad', 'https://example.com:0', 'not-a-url',
                      'https://exa mple.com', 'https://example.com\\evil'):
            with self.subTest(value=value), self.assertRaises(ValueError): websocket_url(value)

    def test_config_fallback_and_environment_priority(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps({'admin_token': 'A'*32,
                'backend_ws_url': 'wss://saved.example/ws/gateways'}))
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(Config.load(path).backend_ws_url, 'wss://saved.example/ws/gateways')
            with patch.dict(os.environ, {'SELFIQ_BACKEND_URL':'https://cloud.example'}, clear=True):
                self.assertEqual(Config.load(path).backend_ws_url, 'wss://cloud.example/ws/gateways')
            with patch.dict(os.environ, {'SELFIQ_BACKEND_URL':'https://cloud.example',
                'SELFIQ_BACKEND_WS_URL':'wss://explicit.example/ws/gateways'}, clear=True):
                self.assertEqual(Config.load(path).backend_ws_url, 'wss://explicit.example/ws/gateways')

    def test_identity_and_token_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            config = replace(Config(), gateway_state_file=directory+'/gateway.json')
            identity = GatewayIdentity(config)
            identifier = config.gateway_id
            public_key = identity.proof('n'*32)['public_key']
            identity.save('https://cloud.example', 'T'*32)
            self.assertEqual(config.backend_ws_url, 'wss://cloud.example/ws/gateways')
            restarted = GatewayIdentity(replace(config, gateway_token=''))
            self.assertEqual(restarted.config.gateway_id, identifier)
            self.assertEqual(restarted.config.gateway_token, 'T'*32)
            self.assertEqual(restarted.proof('n'*32)['public_key'], public_key)


class TLSLinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_verified_tls_registration_command_and_reconnect(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
            now = datetime.now(timezone.utc)
            cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=1))
                .add_extension(x509.SubjectAlternativeName([
                    x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
                .sign(key, hashes.SHA256()))
            (path/'cert.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            (path/'key.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
            trust = ssl.create_default_context(cafile=str(path/'cert.pem'))
            app = signed_backend({'rpi-test':'T'*32}, 'A'*32)
            sock = socket.socket(); sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
            server = uvicorn.Server(uvicorn.Config(app, log_level='error', access_log=False,
                ssl_certfile=str(path/'cert.pem'), ssl_keyfile=str(path/'key.pem')))
            server_task = asyncio.create_task(server.serve(sockets=[sock]))
            link_task = None
            async def execute(command): return {'executed':command}
            config = replace(Config(), gateway_id='rpi-test', gateway_token='T'*32,
                gateway_state_file=str(path/'gateway.json'),
                backend_ws_url=f'https://127.0.0.1:{port}')
            link = BackendLink(config, execute)
            def tls_connect(url, **kwargs): return connect(url, ssl=trust, proxy=None, **kwargs)
            try:
                for _ in range(200):
                    if server.started: break
                    await asyncio.sleep(.01)
                self.assertTrue(server.started)
                with patch('selfiq.backend_link.connect', tls_connect):
                    link_task = asyncio.create_task(link.run())
                    for _ in range(300):
                        if link.connected: break
                        await asyncio.sleep(.01)
                    self.assertTrue(link.connected)
                    old = app.state.sessions['rpi-test']
                    future = asyncio.get_running_loop().create_future()
                    old['pending']['cmd-1'] = future
                    await old['ws'].send_json({'type':'command','id':'cmd-1','command':'status'})
                    result = await asyncio.wait_for(future, 2)
                    self.assertTrue(result['ok'])
                    self.assertEqual(result['data'], {'executed':'status'})
                    await old['ws'].close(code=1012)
                    for _ in range(400):
                        if link.connected and app.state.sessions.get('rpi-test') not in (None, old): break
                        await asyncio.sleep(.01)
                    self.assertTrue(link.connected)
                    self.assertIsNot(app.state.sessions.get('rpi-test'), old)
                    self.assertGreaterEqual(len(app.state.proofs), 2)
            finally:
                if link_task:
                    link_task.cancel(); await asyncio.gather(link_task, return_exceptions=True)
                server.should_exit = True
                await asyncio.wait_for(server_task, 5)
                sock.close()
