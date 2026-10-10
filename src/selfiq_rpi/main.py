import asyncio
import logging
from .api import create_app
from .config import settings
from .network import NetworkManager
from .storage import CredentialStore
from .tcp_server import CredentialTcpServer

logging.basicConfig(level=logging.INFO)


async def run():
    store = CredentialStore(settings.state_file)
    network = NetworkManager(settings.interface)
    tcp = None

    # Si ya existe una configuración, se valida al arrancar. Si falla, el AP
    # sigue disponible para que el usuario pueda corregirla desde la web.
    saved = store.load()
    if saved:
        connected = await network.connect(saved)

    async def stop_setup():
        await tcp.close()
        await network.stop_setup_ap()

    async def credentials_saved():
        tcp.enable_idle_shutdown()

    # Escuchar en todas las interfaces evita una carrera de arranque: hostapd
    # puede tardar unos segundos en asignar la IP del AP a wlan0.
    tcp = CredentialTcpServer(store, "0.0.0.0", settings.tcp_port, settings.ap_idle_seconds, stop_setup)
    await tcp.start()
    import uvicorn
    config = uvicorn.Config(create_app(store, network, credentials_saved), host=settings.http_host, port=settings.http_port, log_level="info")
    await uvicorn.Server(config).serve()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
