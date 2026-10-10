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
from .protocol import HEADER, ACK, NACK, encode, read_frame, credentials_frame

log = logging.getLogger('selfiq')

class TCPService:
    def __init__(self, controller):
        self.ctrl = controller
        self.server = None
        self.clients = set()

    async def handle(self, reader, writer):
        if len(self.clients) >= 8 or not self.ctrl.net.ap_healthy() or self.ctrl.busy:
            writer.close()
            await writer.wait_closed()
            return
        self.clients.add(writer)
        try:
            # Idle clients do not keep provisioning alive.
            for _ in range(32):
                frame = await read_frame(reader)
                if frame.header == HEADER and frame.kind == 0 and frame.action == 1 and frame.data == b'\x00' and self.ctrl.can_distribute():
                    payload = credentials_frame(self.ctrl.creds)
                    self.ctrl.touch()
                    self.ctrl.delivered += 1
                elif frame.header == ACK and frame.kind == 3 and frame.action == 0 and frame.data == b'\x01':
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
        wanted = self.ctrl.net.ap_healthy() and not self.ctrl.busy
        if wanted and self.server is None:
            self.server = await asyncio.start_server(self.handle, self.ctrl.c.ap_ip, self.ctrl.c.tcp_port)
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
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            # Complete in-flight network mutations before shutting children down.
            if jobs:
                await asyncio.gather(*tuple(jobs), return_exceptions=True)
            await tcp.close()
            await asyncio.to_thread(ctrl.shutdown)

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.controller = ctrl
    app.state.tcp = tcp
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

    @app.middleware('http')
    async def restrict(request, call_next):
        try:
            address = peer(request)
            if not address.is_loopback and not (address in ap_subnet and ctrl.net.ap_healthy()):
                admin(request)
            # No CORS. Block cross-site form/fetch submissions and DNS rebinding.
            if request.method != 'GET':
                host = request.headers.get('host', '')
                if host.split(':')[0] not in (config.ap_ip, '127.0.0.1', 'localhost') and request.url.path != '/admin/ap/enable':
                    raise HTTPException(403, 'Host no permitido')
                origin = request.headers.get('origin')
                if origin and origin != 'http://' + host:
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
        html = Path(__file__).with_name('index.html').read_text()
        return html.replace('__SESSION__', session).replace('__AP_IP__', config.ap_ip)

    @app.get('/status')
    async def status():
        return ctrl.status()

    @app.post('/configure', status_code=202)
    async def configure(request: Request):
        if not secrets.compare_digest(request.headers.get('x-selfiq-session', ''), session):
            raise HTTPException(403, 'Recargue la página de configuración')
        if ctrl.busy:
            raise HTTPException(409, 'Ya se está comprobando una red')
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
        if creds.kind == 2 and (not config.enterprise_domain or not Path(config.enterprise_ca).is_file()):
            raise HTTPException(422, 'El instalador debe configurar CA y dominio RADIUS para Enterprise')
        ctrl.busy = True
        async def perform():
            await asyncio.sleep(2)  # Give the HTTP response time to reach the phone.
            await tcp.close()
            await asyncio.to_thread(ctrl.configure, creds)
            await tcp.reconcile()
        job(perform())
        return {'mensaje': 'Comprobando conexión. Si desaparece el AP, reconéctese a SelfIQ-Admi y recargue la página.'}

    @app.post('/admin/ap/enable', status_code=202)
    async def reopen(request: Request):
        admin(request)
        if ctrl.busy:
            raise HTTPException(409, 'Configuración en curso')
        ctrl.busy = True
        async def perform():
            try:
                await asyncio.to_thread(ctrl.reopen)
            except Exception:
                ctrl.error = 'No pudo reabrirse AP; consulte logs'
                log.exception('Fallo al reabrir AP')
            finally:
                ctrl.busy = False
            await tcp.reconcile()
        job(perform())
        return {'mensaje': 'Solicitada apertura del AP'}
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
