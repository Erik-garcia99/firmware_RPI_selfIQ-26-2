"""Persistent device identity and branch token, independent of saved Wi-Fi."""
import base64
from pathlib import Path
import json
from uuid import uuid4
from .backend_config import websocket_url
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
from .models import atomic_json, re_safe_id


def validate_backend(url, token):
    websocket_url(url)
    if not isinstance(token, str) or not 32 <= len(token) <= 255 or any(c.isspace() for c in token):
        raise ValueError('Token de sucursal incorrecto')


class GatewayIdentity:
    def __init__(self, config):
        self.config = config
        path = Path(config.gateway_state_file)
        if path.exists():
            self.data = json.loads(path.read_text())
        else:
            key = Ed25519PrivateKey.generate()
            self.data = {'gateway_id': config.gateway_id or 'rpi-' + uuid4().hex,
                'private_key': base64.b64encode(key.private_bytes(serialization.Encoding.Raw,
                    serialization.PrivateFormat.Raw, serialization.NoEncryption())).decode(),
                'branch_token': config.gateway_token}
            atomic_json(path, self.data)
        if not re_safe_id(self.data['gateway_id']):
            raise ValueError('Identidad guardada inválida; contacte a soporte')
        self.key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(self.data['private_key'], validate=True))
        self.apply()

    def apply(self):
        self.config.gateway_id = self.data['gateway_id']
        self.config.gateway_token = self.data['branch_token']

    def save(self, url, token):
        validate_backend(url, token)
        data = {k:v for k,v in self.data.items() if k != 'backend_ws_url'}
        data['branch_token'] = token
        atomic_json(self.config.gateway_state_file, data)
        self.data = data
        self.apply()
        self.config.backend_ws_url = websocket_url(url)

    def proof(self, nonce):
        if not isinstance(nonce, str) or not 16 <= len(nonce) <= 128:
            raise ValueError('Desafío inválido')
        public_key = self.key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        signature = self.key.sign(('selfiq-gateway-v1:' + self.config.gateway_id + ':' + nonce).encode())
        return {'type': 'enroll', 'branch_token': self.config.gateway_token,
            'public_key': base64.b64encode(public_key).decode(), 'signature': base64.b64encode(signature).decode()}