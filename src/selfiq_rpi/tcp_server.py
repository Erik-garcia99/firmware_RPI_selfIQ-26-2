import asyncio
import logging
import time
from . import protocol

log = logging.getLogger(__name__)


class CredentialTcpServer:
    def __init__(self, store, host: str, port: int, idle_seconds: int, on_idle):
        self.store, self.host, self.port = store, host, port
        self.idle_seconds, self.on_idle = idle_seconds, on_idle
        self.server = None
        self.last_request = time.monotonic()
        self._idle_task = None

    async def start(self):
        self.server = await asyncio.start_server(self._client, self.host, self.port)
        self._idle_task = asyncio.create_task(self._watch_idle())

    async def close(self):
        if self._idle_task:
            self._idle_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _client(self, reader, writer):
        try:
            packet = await asyncio.wait_for(reader.read(256), 10)
            frame = protocol.parse_frame(packet)
            self.last_request = time.monotonic()
            if frame.header != protocol.HEADER or frame.action != protocol.REQ_WIFI:
                writer.write(protocol.nack())
            else:
                credentials = self.store.load()
                if not credentials:
                    writer.write(protocol.nack())
                else:
                    writer.write(protocol.credential_response(credentials["ssid"], credentials["password"], credentials.get("username")))
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionError, protocol.ProtocolError) as error:
            log.warning("Peticion TCP invalida: %s", error)
            writer.write(protocol.nack())
            try:
                await writer.drain()
            except ConnectionError:
                pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def _watch_idle(self):
        while True:
            await asyncio.sleep(5)
            if time.monotonic() - self.last_request >= self.idle_seconds:
                await self.on_idle()
                return
