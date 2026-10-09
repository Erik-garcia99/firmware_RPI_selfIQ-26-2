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
        await network.connect(saved)

    async def stop_setup():
        await tcp.close()
        await network.stop_setup_ap()

    tcp = CredentialTcpServer(store, settings.ap_ip, settings.tcp_port, settings.ap_idle_seconds, stop_setup)
    await tcp.start()
    import uvicorn
    config = uvicorn.Config(create_app(store, network, lambda: asyncio.sleep(0)), host=settings.http_host, port=settings.http_port, log_level="info")
    await uvicorn.Server(config).serve()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
