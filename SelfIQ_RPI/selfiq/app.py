import asyncio
from contextlib import asynccontextmanager
import ipaddress
import logging
from pathlib import Path
import secrets

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from .models import Config, Credentials
from .network import Network
from .controller import Controller
from .backend_link import BackendLink
from .backend_config import websocket_url
from .mqtt_bridge import MQTTBridge
from .calibration import (CalibrationCommandError, ReferenceSpoolFull,
                          parse_multipart_reference)
from .influx_sync import run_influx_sync
from .protocol import HEADER, ACK, NACK, encode, read_frame, credentials_frame


log = logging.getLogger('selfiq')

class TCPService:
    def __init__(self, controller):
        self.ctrl = controller
        self.server = None
        self.clients = set()
        self.health = lambda: (False, False, False)

    async def handle(self, reader, writer):
        if len(self.clients) >= 8 or not self.ctrl.ap_wanted or not self.ctrl.net.ap_healthy() or self.ctrl.busy:
            writer.close()
            await writer.wait_closed()
            return
        self.clients.add(writer)
        try:
            # Idle clients do not keep provisioning alive.
            for _ in range(32):
                frame = await read_frame(reader)
                if frame.header == HEADER and frame.kind == 1 and frame.action == 1 and frame.data in (b'', b'\x00'):
                    creds = await asyncio.to_thread(self.ctrl.requested_credentials)
                    payload = credentials_frame(creds) if creds else NACK
                elif frame.header == HEADER and frame.kind == 4 and frame.action == 1 and not frame.data:
                    await asyncio.to_thread(self.ctrl.touch)
                    mqtt_ok, backend_ok, storage_ok = self.health()
                    # v1: version, wifi, backend, mqtt, storage, action (1=stay,2=try STA)
                    payload = encode(ACK, 4, 2, bytes((1, int(self.ctrl.online),
                        int(backend_ok), int(mqtt_ok), int(storage_ok),
                        2 if self.ctrl.can_distribute() else 1)))
                elif frame.header == ACK and frame.kind == 3 and frame.action == 0:
                    # Receipt acknowledgment, not proof ESP joined the final Wi-Fi.
                    break
                else:
                    payload = NACK
                writer.write(payload)
                await writer.drain()
        except (ValueError, asyncio.TimeoutError):
            try:
                writer.write(NACK)
                await writer.drain()
            except (OSError, ConnectionError):
                pass
        except (asyncio.IncompleteReadError, OSError, ConnectionError):
            pass
        finally:
            self.clients.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def reconcile(self):
        wanted = self.ctrl.can_distribute()
        if wanted and self.server is None:
            last_error = None
            for _ in range(5):
                try:
                    self.server = await asyncio.start_server(
                        self.handle, self.ctrl.c.ap_ip, self.ctrl.c.tcp_port,
                        reuse_address=True)
                    last_error = None
                    break
                except OSError as exc:
                    last_error = exc
                    # El socket anterior puede quedar en TIME_WAIT al bajar/subir el AP.
                    # Reintentamos antes de declarar el TCP inutilizable.
                    await asyncio.sleep(0.25)
            if last_error is not None:
                raise last_error
        elif not wanted:
            await self.close()

    async def close(self):
        server = self.server
        if self.server:
            self.server.close()
            self.server = None
        for writer in tuple(self.clients):
            writer.close()
        if server:
            await server.wait_closed()

def create_app(config, network=None):
    ctrl = Controller(config, network or Network(config))
    tcp = TCPService(ctrl)
    jobs = set()

    async def execute(command, message=None):
        if command in {'snapshot.request', 'roi.set', 'light.test', 'calibrate',
                       'calibration.commit', 'calibration.cancel', 'calibration.status'}:
            body = message or {}
            if command in {'calibrate', 'calibration.commit', 'calibration.cancel',
                           'calibration.status'}:
                body = body.get('data') if isinstance(body.get('data'), dict) else {}
            try:
                registered_at = await asyncio.to_thread(
                    mqtt_bridge.authorize_node, body.get('branch_id'), body.get('node_id'))
            except Exception:
                raise ValueError('No se pudo verificar el registro del nodo') from None
            if registered_at is None:
                raise ValueError('Nodo no confirmado en el sistema')
        if command == 'status':
            # Backend receives operational state, never SSID/password/username.
            state = ctrl.status()
            state.pop('ssid', None)
            return {**state, 'tcp_activo': tcp.server is not None,
                    'backend_connected': backend.connected,
                    'storage_error': mqtt_bridge.storage_error,
                    'outbox': mqtt_bridge.outbox.status() if mqtt_bridge.outbox else None}
        if command == 'node.reset':
            return await asyncio.to_thread(mqtt_bridge.reset_node, message or {})
        if command == 'snapshot.request':
            return await asyncio.to_thread(mqtt_bridge.request_snapshot, message or {})
        if command == 'roi.set':
            return await asyncio.to_thread(mqtt_bridge.set_rois, message or {})
        if command == 'light.test':
            # Solo se confirma la entrega al broker local; el firmware actual
            # del nodo todavía no acciona GPIO de luces.
            return await asyncio.to_thread(mqtt_bridge.test_light, message or {})
        if command in {'calibrate', 'calibration.commit', 'calibration.cancel',
                       'calibration.status'}:
            body = (message or {}).get('data')
            if not isinstance(body, dict):
                raise ValueError('Comando de calibración sin data')
            try:
                return await asyncio.to_thread(mqtt_bridge.run_calibration, body)
            except CalibrationCommandError as exc:
                raise ValueError(str(exc)) from None
        if ctrl.busy:
            raise ValueError('Configuración en curso')
        if not ctrl.online or not ctrl.creds:
            raise ValueError('Wi-Fi no conectado; configure la Raspberry desde su AP')
        ctrl.busy = True
        try:
            if command == 'tcp.enable':
                await asyncio.to_thread(ctrl.reopen)
            elif command == 'tcp.disable':
                if ctrl.recovery_ap:
                    raise ValueError('AP en recuperacion; hay clientes locales o periodo de espera')
                await tcp.close()
                await asyncio.to_thread(ctrl.stop_provisioning)
            else:
                raise ValueError('Orden no permitida')
        finally:
            ctrl.busy = False
        await tcp.reconcile()
        if command == 'tcp.enable' and tcp.server is None:
            raise ValueError('No se pudo iniciar TCP')
        return await execute('status')

    backend = BackendLink(config, execute)
    mqtt_bridge = MQTTBridge(config, backend)
    tcp.health = lambda: (mqtt_bridge.connected, backend.connected,
                         mqtt_bridge.outbox is not None and not mqtt_bridge.storage_error)

    def job(coro):
        task = asyncio.create_task(coro)
        jobs.add(task)
        task.add_done_callback(jobs.discard)
        return task

    async def monitor():
        while True:
            try:
                await asyncio.to_thread(ctrl.tick)
                await tcp.reconcile()
            except Exception:
                ctrl.error = 'No se pudo iniciar/restablecer AP/TCP; consulte journalctl'
                log.exception('Fallo de supervisión')
            await asyncio.sleep(5)

    @asynccontextmanager
    async def lifespan(app):
        await asyncio.to_thread(ctrl.bootstrap)
        await tcp.reconcile()

        task = asyncio.create_task(monitor())
        backend_task = asyncio.create_task(backend.run())
        log.info("SelfIQ: cargando puente MQTT de registro ESP32")
        await mqtt_bridge.start()
        influx_stop = asyncio.Event()
        influx_task = None
        if mqtt_bridge.outbox is not None:
            influx_task = asyncio.create_task(
                run_influx_sync(
                    config,
                    mqtt_bridge.outbox,
                    influx_stop,
                    mqtt_bridge.authorize_node,
                ),
                name="selfiq-influx-sync",
            )
        else:
            log.error(
                "[INFLUX] No se inicia sincronización: SQLite no disponible"
            )
        try:
            yield

        finally:
            task.cancel()
            # Esperar cualquier cambio de red que esté en curso.
            if ctrl.busy:
                await asyncio.gather(
                    *tuple(jobs),
                    return_exceptions=True,
                )
            # Terminar el envío antes de cerrar SQLite.
            # No cancelar el hilo mientras utiliza la base de datos.
            influx_stop.set()

            if influx_task is not None:
                await asyncio.gather(
                    influx_task,
                    return_exceptions=True,
                )

            await mqtt_bridge.stop()
            backend_task.cancel()

            await asyncio.gather(
                task,
                backend_task,
                return_exceptions=True,
            )

            if jobs:
                await asyncio.gather(
                    *tuple(jobs),
                    return_exceptions=True,
                )

            await tcp.close()
            await asyncio.to_thread(ctrl.shutdown)

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.controller = ctrl
    app.state.tcp = tcp
    app.state.backend = backend
    app.state.mqtt_bridge = mqtt_bridge
    app.state.execute_command = execute
    session = secrets.token_urlsafe(32)
    ap_subnet = ipaddress.ip_network(config.ap_ip + '/24', strict=False)

    def peer(request):
        try:
            return ipaddress.ip_address(request.client.host)
        except (ValueError, AttributeError):
            raise HTTPException(403, 'Origen no permitido')

    def admin(request):
        if not secrets.compare_digest(request.headers.get('x-selfiq-token', ''), config.admin_token):
            raise HTTPException(403, 'Token administrativo requerido')

    # Rutas que el ESP32 puede llamar sin token admin (red local).
    CAMERA_PATHS = {'/camera/snapshot', '/camera/calibration-reference'}

    @app.middleware('http')
    async def restrict(request, call_next):
        try:
            address = peer(request)
            if not address.is_loopback and not (address in ap_subnet and ctrl.net.ap_healthy()) \
                    and request.url.path not in CAMERA_PATHS:
                admin(request)
            # No CORS. Block cross-site form/fetch submissions and DNS rebinding.
            if request.method != 'GET':
                host = request.headers.get('host', '')
                if (host.split(':')[0] not in (config.ap_ip, '127.0.0.1', 'localhost')
                        and request.url.path not in ('/admin/ap/enable', '/admin/tcp/enable',
                                                    '/admin/tcp/disable', '/admin/backend/configure')
                        and request.url.path not in CAMERA_PATHS):
                    raise HTTPException(403, 'Host no permitido')
                origin = request.headers.get('origin')
                if origin and origin != 'http://' + host and request.url.path not in CAMERA_PATHS:
                    raise HTTPException(403, 'Origen no permitido')
            response = await call_next(request)
        except HTTPException as exc:
            response = JSONResponse({'detail': exc.detail}, status_code=exc.status_code)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Frame-Options'] = 'DENY'
        return response

    @app.get('/', response_class=HTMLResponse)
    async def index():
        if not ctrl.configuration_required():
            raise HTTPException(409, 'Wi-Fi ya configurado; active TCP para agregar una ESP32')
        html = Path(__file__).with_name('index.html').read_text()
        return html.replace('__SESSION__', session).replace('__AP_IP__', config.ap_ip)

    @app.get('/admin/mqtt/status')
    async def mqtt_status(request: Request):
        admin(request)
        return {'broker_connected': mqtt_bridge.connected,
                'gateway_registered': backend.connected,
                'pending_registrations': len(mqtt_bridge.pending),
                'node_type_for_plain_mac': mqtt_bridge.default_node_type}

    @app.get('/status')
    async def status():
        return {'storage_error': mqtt_bridge.storage_error,
                'outbox': mqtt_bridge.outbox.status() if mqtt_bridge.outbox else None,
                **ctrl.status(), 'tcp_activo': tcp.server is not None, 'backend_conectado': backend.connected,
                'gateway_id': config.gateway_id, 'branch_id': backend.branch_id,
                'registration_status': backend.registration_status,
                'registration_alert': 'Corrija el token de sucursal' if backend.registration_status in ('token_invalid', 'registration_rejected') else None}

    # Almacenamiento en memoria de la ultima imagen por nodo.
    _snapshots = {}

    @app.post('/camera/snapshot')
    async def receive_snapshot(request: Request):
        node_id = request.headers.get('x-node-id', '')
        branch_id = request.headers.get('x-branch-id', '')
        req_id = request.headers.get('x-req-id', '')
        if not node_id:
            raise HTTPException(400, 'Falta X-Node-Id')
        try:
            registered_at = await asyncio.to_thread(mqtt_bridge.authorize_node, branch_id, node_id)
        except Exception:
            raise HTTPException(503, 'No se pudo verificar el registro del nodo') from None
        if registered_at is None:
            raise HTTPException(403, 'Nodo no confirmado en el sistema')
        raw = await request.body()
        if not raw or len(raw) > 200000:
            raise HTTPException(413, 'Imagen demasiado grande o vacia')
        _snapshots[node_id] = {
            'data': raw,
            'req_id': req_id,
            'branch_id': branch_id,
            'received_at': __import__('time').time(),
        }
        log.info('Snapshot recibido: nodo=%s req=%s bytes=%d', node_id, req_id, len(raw))
        # Notificar al waiter del comando snapshot.request
        if req_id:
            mqtt_bridge.notify_snapshot(node_id, req_id)

        # Reenviar al backend firmado (Ed25519) en segundo plano.
        async def _forward():
            try:
                if not backend.connected:
                    log.warning('Snapshot no reenviado: backend desconectado')
                    return
                if await asyncio.to_thread(mqtt_bridge.authorize_node, branch_id, node_id) is None:
                    log.warning('Snapshot no reenviado: nodo no autorizado')
                    return
                identity = backend.ensure_identity()
                import time as _t, secrets as _s, base64 as _b64, hashlib as _hl
                from urllib.request import Request as _Req, urlopen as _urlopen
                from urllib.parse import urlsplit, urlunsplit, quote as _quote
                ts = int(_t.time())
                nonce = _s.token_urlsafe(18)
                img_hash = _hl.sha256(raw).hexdigest()
                data = (f'selfiq-snapshot-v1\\n{config.gateway_id}\\n{ts}\\n{nonce}\\n{node_id}\\n{img_hash}').encode()
                sig = _b64.b64encode(identity.key.sign(data)).decode()
                base = urlsplit(websocket_url(config.backend_ws_url))
                scheme = 'https' if base.scheme == 'wss' else 'http'
                prefix = (base.path.removesuffix('/ws/gateways') + '/gateways/'
                          + _quote(config.gateway_id, safe=''))
                url = urlunsplit((scheme, base.netloc, prefix + '/devices/' + _quote(node_id, safe='') + '/snapshot', '', ''))
                req = _Req(url, data=raw, headers={
                    'Content-Type': 'image/jpeg',
                    'X-Timestamp': str(ts),
                    'X-Nonce': nonce,
                    'X-Signature': sig,
                    'X-Gateway-Id': config.gateway_id,
                }, method='POST')
                with _urlopen(req, timeout=15) as resp:
                    log.info('Snapshot reenviado al backend: %s (HTTP %s)', node_id, resp.status)
            except Exception as exc:
                log.warning('No se pudo reenviar snapshot al backend: %s: %s',
                            type(exc).__name__, exc)

        import asyncio as _asyncio
        _asyncio.create_task(_forward())

        return {'ok': True, 'bytes': len(raw), 'req_id': req_id}

    @app.post('/camera/calibration-reference')
    async def receive_calibration_reference(request: Request):
        node_id = request.headers.get('x-node-id', '')
        branch_id = request.headers.get('x-branch-id', '')
        if not node_id or not branch_id:
            raise HTTPException(400, 'Faltan X-Node-Id/X-Branch-Id')
        try:
            registered_at = await asyncio.to_thread(
                mqtt_bridge.authorize_node, branch_id, node_id)
        except Exception:
            raise HTTPException(503, 'No se pudo verificar el registro del nodo') from None
        if registered_at is None:
            raise HTTPException(403, 'Nodo no confirmado en el sistema')
        raw_request = await request.body()
        try:
            meta, image = parse_multipart_reference(
                request.headers.get('content-type', ''), raw_request)
            state, reference = await asyncio.to_thread(
                mqtt_bridge.accept_calibration_reference,
                node_id, branch_id, meta, image)
        except ReferenceSpoolFull:
            raise HTTPException(507, 'Spool de referencias lleno') from None
        except CalibrationCommandError as exc:
            status = 409 if exc.code in ('operation_conflict', 'calibration_not_found') else 422
            if exc.code == 'storage_full':
                status = 507
            raise HTTPException(status, {'code': exc.code, 'detail': exc.detail}) from None
        return {
            'ok': True,
            'capture_id': meta.get('capture_id'),
            'status': state,
            'queued': reference.get('status') in ('pending', 'uploading'),
        }

    @app.get('/camera/snapshot')
    async def get_snapshot(request: Request):
        node_id = request.query_params.get('node_id', '')
        if not node_id or node_id not in _snapshots:
            raise HTTPException(404, 'Sin imagen para este nodo')
        snap = _snapshots[node_id]
        try:
            registered_at = await asyncio.to_thread(
                mqtt_bridge.authorize_node, snap['branch_id'], node_id)
        except Exception:
            raise HTTPException(503, 'No se pudo verificar el registro del nodo') from None
        if registered_at is None:
            _snapshots.pop(node_id, None)
            raise HTTPException(403, 'Nodo no confirmado en el sistema')
        from fastapi.responses import Response as RawResponse
        return RawResponse(content=snap['data'], media_type='image/jpeg',
                           headers={'X-Received-At': str(snap['received_at']),
                                    'X-Req-Id': snap['req_id']})

    @app.post('/configure', status_code=202)
    async def configure(request: Request):
        if not secrets.compare_digest(request.headers.get('x-selfiq-session', ''), session):
            raise HTTPException(403, 'Recargue la página de configuración')
        if ctrl.busy:
            raise HTTPException(409, 'Ya se está comprobando una red')
        if not ctrl.configuration_required():
            raise HTTPException(409, 'Wi-Fi conectado; no es necesario cambiar sus credenciales')
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 2048:
                raise HTTPException(413, 'Solicitud demasiado grande')
        try:
            import json
            creds = Credentials(**json.loads(raw)).validate()
        except (ValueError, TypeError):
            raise HTTPException(422, 'Credenciales inválidas: revise tipo y longitudes en bytes')
        ctrl.busy = True
        async def perform():
            await asyncio.sleep(2)  # Give the HTTP response time to reach the phone.
            await tcp.close()
            await asyncio.to_thread(ctrl.configure, creds)
            await tcp.reconcile()
        job(perform())
        return {'mensaje': 'Comprobando conexión. Si desaparece el AP, reconéctese a SelfIQ-Admi y recargue la página.'}

    @app.post('/backend/configure')
    @app.post('/admin/backend/configure')
    async def configure_backend(request: Request):
        if request.url.path.startswith('/admin/'):
            admin(request)
        elif not secrets.compare_digest(request.headers.get('x-selfiq-session', ''), session):
            raise HTTPException(403, 'Recargue la página de configuración')
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 2048:
                raise HTTPException(413, 'Solicitud demasiado grande')
        try:
            import json
            body = json.loads(raw)
            if not config.backend_ws_url:
                raise HTTPException(503, 'El equipo requiere configuración del servicio; contacte a soporte')
            await backend.configure(config.backend_ws_url, body['branch_token'])
        except (ValueError, TypeError, KeyError):
            raise HTTPException(422, 'Revise el token de sucursal') from None
        return {'message':'Token guardado; comprobando registro sin cambiar Wi-Fi', 'gateway_id': config.gateway_id}

    @app.post('/admin/ap/enable', status_code=202)
    @app.post('/admin/tcp/enable', status_code=202)
    async def reopen(request: Request):
        admin(request)
        if ctrl.busy:
            raise HTTPException(409, 'Configuración en curso')
        if not ctrl.online or not ctrl.creds:
            raise HTTPException(409, 'Wi-Fi no conectado; use la página de configuración del AP')
        ctrl.busy = True
        async def perform():
            try:
                await asyncio.to_thread(ctrl.reopen)
            except ValueError as exc:
                ctrl.error = str(exc)
            except Exception:
                ctrl.error = 'No pudo reabrirse AP; consulte logs'
                log.exception('Fallo al reabrir AP')
            finally:
                ctrl.busy = False
            await tcp.reconcile()
        job(perform())
        return {'mensaje': 'Solicitada apertura de AP + TCP con las credenciales Wi-Fi guardadas',
                'inactividad_segundos': config.idle_seconds}

    @app.post('/admin/tcp/disable')
    async def stop_provisioning(request: Request):
        admin(request)
        if ctrl.busy:
            raise HTTPException(409, 'Configuración en curso')
        if not ctrl.online or ctrl.recovery_ap:
            raise HTTPException(409, 'El AP debe permanecer disponible durante recuperacion')
        ctrl.busy = True
        try:
            await tcp.close()
            await asyncio.to_thread(ctrl.stop_provisioning)
        finally:
            ctrl.busy = False
        return {'mensaje': 'TCP y AP cerrados; credenciales Wi-Fi conservadas'}
    return app

def main():
    import argparse
    import uvicorn
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='/etc/selfiq/config.json')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(create_app(Config.load(args.config)), host='0.0.0.0', port=80,
                proxy_headers=False, access_log=False, workers=1)

if __name__ == '__main__':
    main()
