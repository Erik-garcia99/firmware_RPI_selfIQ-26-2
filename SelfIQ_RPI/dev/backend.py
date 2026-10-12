"""Backend mínimo para probar el canal; integrar estas rutas en el backend real."""
import asyncio
import json
import os
import secrets
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Literal


class Command(BaseModel):
    command: Literal['tcp.enable', 'tcp.disable', 'status']


def create_app(gateways=None, api_token=None, timeout=20):
    gateways = gateways if gateways is not None else json.loads(os.environ['GATEWAY_TOKENS'])
    api_token = api_token if api_token is not None else os.environ['DEV_API_TOKEN']
    if not gateways or len(api_token) < 32 or any(len(t) < 32 for t in gateways.values()):
        raise ValueError('Configure tokens de al menos 32 caracteres')
    app = FastAPI()
    sessions = {}
    app.state.sessions = sessions

    def admin(request):
        if not secrets.compare_digest(request.headers.get('x-selfiq-token', ''), api_token):
            raise HTTPException(403, 'Token de API requerido')

    @app.get('/health')
    async def health():
        return {'ok': True}

    @app.get('/gateways/{gateway_id}')
    async def state(gateway_id: str, request: Request):
        admin(request)
        if gateway_id not in gateways:
            raise HTTPException(404, 'Gateway desconocido')
        return {'gateway_id': gateway_id, 'connected': gateway_id in sessions}

    @app.websocket('/ws/gateways/{gateway_id}')
    async def link(ws: WebSocket, gateway_id: str):
        expected = gateways.get(gateway_id)
        if not expected or not secrets.compare_digest(ws.headers.get('authorization', ''), 'Bearer ' + expected):
            await ws.close(code=1008)
            return
        await ws.accept()
        previous = sessions.get(gateway_id)
        if previous:
            await previous['ws'].close(code=1012)
        session = {'ws': ws, 'pending': {}, 'send_lock': asyncio.Lock()}
        sessions[gateway_id] = session
        try:
            while True:
                raw = await ws.receive_text()
                if len(raw.encode()) > 8192:
                    await ws.close(code=1009)
                    break
                try:
                    response = json.loads(raw)
                except ValueError:
                    await ws.close(code=1008)
                    break
                if not isinstance(response, dict):
                    await ws.close(code=1008)
                    break
                identifier = response.get('id')
                future = session['pending'].get(identifier) if isinstance(identifier, str) else None
                if response.get('type') == 'result' and future and not future.done():
                    future.set_result(response)
        except WebSocketDisconnect:
            pass
        finally:
            if sessions.get(gateway_id) is session:
                sessions.pop(gateway_id, None)
            for future in session['pending'].values():
                if not future.done():
                    future.set_result(None)

    @app.post('/gateways/{gateway_id}/commands')
    async def command(gateway_id: str, body: Command, request: Request):
        admin(request)
        if gateway_id not in gateways:
            raise HTTPException(404, 'Gateway desconocido')
        session = sessions.get(gateway_id)
        if session is None:
            raise HTTPException(503, 'Raspberry desconectada; no se encola la orden')
        if len(session['pending']) >= 16:
            raise HTTPException(429, 'Demasiadas órdenes pendientes')
        identifier = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        session['pending'][identifier] = future
        try:
            async with session['send_lock']:
                await session['ws'].send_json({'type': 'command', 'id': identifier, 'command': body.command})
            response = await asyncio.wait_for(future, timeout)
            if response is None:
                raise HTTPException(503, 'Se perdió la conexión; consulte estado antes de repetir')
            return response  # ok=false means RPi rejected/failed the operation.
        except asyncio.TimeoutError:
            raise HTTPException(504, 'Sin confirmación; consulte estado antes de repetir')
        except (RuntimeError, OSError, WebSocketDisconnect):
            raise HTTPException(503, 'No se pudo enviar; consulte estado antes de repetir')
        finally:
            session['pending'].pop(identifier, None)

    return app
