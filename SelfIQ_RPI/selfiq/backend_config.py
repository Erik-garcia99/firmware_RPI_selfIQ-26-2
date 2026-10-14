"""Resolve the public backend endpoint without embedding deployment secrets."""
import ipaddress
from urllib.parse import urlsplit, urlunsplit


def websocket_url(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Ingrese el dominio público HTTPS de Railway')
    parsed = urlsplit(value.strip())
    if (parsed.scheme not in ('https', 'wss', 'http', 'ws') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment):
        raise ValueError('URL requiere https:// o wss:// sin credenciales, query ni fragmento')
    try:
        port = parsed.port
    except ValueError:
        raise ValueError('Puerto de backend inválido') from None
    if port == 0:
        raise ValueError('Puerto de backend inválido')
    hostname = parsed.hostname.lower()
    if any(c.isspace() for c in value) or '\\' in value:
        raise ValueError('URL contiene caracteres no permitidos')
    if hostname.endswith('.railway.internal'):
        raise ValueError('Use el dominio público de Railway, no railway.internal')
    if parsed.scheme in ('http', 'ws'):
        try:
            local = ipaddress.ip_address(hostname).is_private
        except ValueError:
            local = hostname in ('localhost', 'backend')  # simulador Docker existente
        if not local:
            raise ValueError('El backend en Internet requiere HTTPS/WSS')
    path = parsed.path.rstrip('/')
    if not path.endswith('/ws/gateways'):
        path += '/ws/gateways'
    scheme = 'wss' if parsed.scheme in ('https', 'wss') else 'ws'
    return urlunsplit((scheme, parsed.netloc, path, '', ''))
