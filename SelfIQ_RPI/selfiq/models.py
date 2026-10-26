from dataclasses import dataclass, asdict
from pathlib import Path
import json
import os
import tempfile

@dataclass(frozen=True)
class Credentials:
    ssid: str
    password: str
    kind: int = 1
    username: str = ''

    def validate(self):
        for value in (self.ssid, self.password, self.username):
            if not isinstance(value, str) or any(c in value for c in ('\x00', '\r', '\n')):
                raise ValueError('Campos con caracteres no permitidos')
        if not 1 <= len(self.ssid.encode()) <= 32:
            raise ValueError('SSID: entre 1 y 32 bytes UTF-8')
        if self.kind == 1:
            if not 8 <= len(self.password.encode()) <= 63 or self.username:
                raise ValueError('WPA2 Personal: contraseña de 8–63 bytes; sin usuario')
        else:
            raise ValueError('Tipo de red desconocido')
        return self

def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(value, output, ensure_ascii=False)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

@dataclass
class Config:
    sta_interface: str
    ap_interface: str
    virtual_ap: bool
    ap_ip: str
    ap_ssid: str
    ap_password: str
    country: str
    setup_channel: int
    idle_seconds: int
    connect_timeout: int
    retry_seconds: int
    recovery_rounds: int
    recovery_attempts: int
    recovery_retry_seconds: int
    offline_ap_delay: int
    offline_db: str
    offline_max_fraction: float
    offline_reserve_fraction: float
    offline_reserve_bytes: int
    offline_max_payload: int
    admin_token: str
    state_file: str
    runtime_dir: str
    tcp_port: int
    backend_ws_url: str
    gateway_id: str
    gateway_token: str
    gateway_state_file: str

    influx_url: str
    influx_bucket: str
    influx_org: str
    influx_token: str

    
    @classmethod
    def load(cls, path):
        settings = json.loads(Path(path).read_text())
        settings.pop('enterprise_ca', None)
        settings.pop('enterprise_domain', None)
        result = cls(**settings)
        # Optional outbound backend link; existing installations remain compatible.
        for field in ('gateway_id', 'gateway_token'):
            setattr(result, field, os.environ.get('SELFIQ_' + field.upper(), getattr(result, field)))
        # Prefer explicit WS endpoint, then public HTTPS base URL, then config.json.
        # An unset environment no longer erases the saved URL.
        source = (os.environ.get('SELFIQ_BACKEND_WS_URL', '').strip()
                  or os.environ.get('SELFIQ_BACKEND_URL', '').strip()
                  or result.backend_ws_url)
        if source:
            from .backend_config import websocket_url
            result.backend_ws_url = websocket_url(source)
        if result.gateway_id and not re_safe_id(result.gateway_id):
            raise ValueError('gateway_id incorrecto')
        # Missing/invalid enrollment token must not stop Wi-Fi provisioning or the service.
        import ipaddress, re
        ip = ipaddress.ip_address(result.ap_ip)
        if ip.version != 4 or not ip.is_private or str(ip).split('.')[-1] != '1':
            raise ValueError('AP requiere una IPv4 privada terminada en .1 (/24)')
        if result.sta_interface == result.ap_interface:
            raise ValueError('STA y AP necesitan nombres de interfaz distintos')
        for interface in (result.sta_interface, result.ap_interface):
            if not re.fullmatch(r'[a-zA-Z0-9_-]{1,15}', interface):
                raise ValueError('Interfaz incorrecta')
        if not re.fullmatch('[A-Z]{2}', result.country):
            raise ValueError('País incorrecto')
        if not 1 <= result.setup_channel <= 11 or result.idle_seconds < 1 or result.retry_seconds < 1:
            raise ValueError('Canal o tiempos incorrectos')
        if not (0 < result.offline_max_fraction < 1 and
                0 < result.offline_reserve_fraction < 1):
            raise ValueError('Fracciones de almacenamiento invalidas')
        if (min(result.recovery_rounds, result.recovery_attempts,
                result.recovery_retry_seconds, result.offline_ap_delay,
                result.offline_max_payload, result.connect_timeout) < 1
                or result.offline_reserve_bytes < 0):
            raise ValueError('Limites de recuperacion invalidos')
        Credentials(result.ap_ssid, result.ap_password).validate()
        if len(result.admin_token) < 32:
            raise ValueError('Falta token administrativo de al menos 32 caracteres')
        return result

def re_safe_id(value):
    import re
    return isinstance(value, str) and bool(re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', value))