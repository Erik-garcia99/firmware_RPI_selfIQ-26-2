"""Envío HTTP a InfluxDB; configuración recibida desde Config.load().

Solo transporte: no convierte JSON MQTT, no borra SQLite ni inicia tareas.
El llamador debe conservar los pendientes si se lanza una excepción.
"""
from http import HTTPStatus
import math
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler


WRITE_PATH = '/api/v2/write'
TIMESTAMP_PRECISION = 'ns'
DEFAULT_TIMEOUT_SECONDS = 10.0


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # No reenviar el token a otro destino mediante redirecciones.
        return None


def enviar_influx(config, line_protocol: str,
                  timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
    """Envía una o varias líneas con timestamps Unix en nanosegundos.

    Devuelve True al recibir HTTP 204 (aceptado por la API).
    HTTPError/URLError/TimeoutError se propagan para conservar el pendiente.
    En Cloud TSM, la aceptación no garantiza consulta inmediata.
    No genera timestamps: reutilice el original en cada reintento.
    """
    if not isinstance(line_protocol, str) or not line_protocol.strip():
        raise ValueError('No hay datos en line protocol para enviar')
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('El timeout debe ser positivo y finito')

    # Las cuatro propiedades proceden del JSON cargado por Config.load().
    url_base = config.influx_url.rstrip('/')
    bucket = config.influx_bucket
    org = config.influx_org
    token = config.influx_token

    host = urlsplit(url_base)
    if (host.scheme != 'https' or not host.hostname or host.username is not None
            or host.password is not None or host.path or host.query or host.fragment):
        raise ValueError('influx_url debe ser el host HTTPS, sin rutas ni credenciales')
    if not isinstance(bucket, str) or not bucket.strip():
        raise ValueError('Falta influx_bucket')
    if not isinstance(token, str) or not token.strip() or '\r' in token or '\n' in token:
        raise ValueError('influx_token ausente o inválido')
    if not isinstance(org, str):
        raise ValueError('influx_org debe ser una cadena')

    parametros = urlencode({'bucket': bucket, 'org': org,
                            'precision': TIMESTAMP_PRECISION})
    solicitud = Request(
        url=f'{url_base}{WRITE_PATH}?{parametros}',
        data=line_protocol.encode('utf-8'),
        headers={'Authorization': f'Token {token}',
                 'Content-Type': 'text/plain; charset=utf-8'},
        method='POST',
    )
    with build_opener(_NoRedirect()).open(solicitud, timeout=timeout) as respuesta:
        if respuesta.status != HTTPStatus.NO_CONTENT:
            raise RuntimeError(f'Respuesta inesperada de InfluxDB: HTTP {respuesta.status}')
    return True
