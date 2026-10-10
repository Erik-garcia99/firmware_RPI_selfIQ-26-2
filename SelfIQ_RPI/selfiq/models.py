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
        elif self.kind == 2:
            if not 1 <= len(self.username.encode()) <= 31 or not 1 <= len(self.password.encode()) <= 64:
                raise ValueError('Enterprise: usuario de 1–31 y contraseña de 1–64 bytes')
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
    sta_interface: str = 'wlan0'
    ap_interface: str = 'uap0'
    virtual_ap: bool = True
    ap_ip: str = '192.168.50.1'
    ap_ssid: str = 'SelfIQ-Admi'
    ap_password: str = '$elfIQ-4dmi-1'
    country: str = 'MX'
    setup_channel: int = 6
    idle_seconds: int = 300
    connect_timeout: int = 45
    retry_seconds: int = 60
    enterprise_ca: str = '/etc/ssl/certs/ca-certificates.crt'
    enterprise_domain: str = ''
    admin_token: str = ''
    state_file: str = '/var/lib/selfiq/credentials.json'
    runtime_dir: str = '/run/selfiq'
    tcp_port: int = 5000

    @classmethod
    def load(cls, path):
        result = cls(**json.loads(Path(path).read_text()))
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
        Credentials(result.ap_ssid, result.ap_password).validate()
        if len(result.admin_token) < 32:
            raise ValueError('Falta token administrativo de al menos 32 caracteres')
        return result
