from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse
from .config import settings


def create_app(store, network, on_credentials):
    app = FastAPI(title="SelfIQ Raspberry Pi setup")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return """<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'>
        <title>SelfIQ - Configuracion WiFi</title><h1>Configurar SelfIQ</h1>
        <form method=post action=/wifi><label>Tipo <select name=type><option value=normal>Normal</option><option value=enterprise>Empresa</option></select></label><br>
        <label>SSID <input name=ssid required maxlength=32></label><br><label>Contrasena <input name=password type=password required maxlength=64></label><br>
        <label>Usuario (empresa) <input name=username maxlength=64></label><br><button>Conectar y guardar</button></form>"""

    @app.post("/wifi")
    async def wifi(type: str = Form("normal"), ssid: str = Form(...), password: str = Form(...), username: str = Form("")):
        if not 1 <= len(ssid) <= 32 or not 1 <= len(password) <= 64:
            raise HTTPException(400, "SSID o contrasena invalida")
        if type == "enterprise" and not username:
            raise HTTPException(400, "El usuario es obligatorio para red empresarial")
        credentials = {"type": type, "ssid": ssid, "password": password}
        if type == "enterprise":
            credentials["username"] = username
        if not await network.connect(credentials):
            raise HTTPException(409, "No fue posible conectarse a la red")
        store.save(credentials)
        await on_credentials()
        return {"ok": True, "message": "WiFi conectado y credenciales guardadas"}

    return app
