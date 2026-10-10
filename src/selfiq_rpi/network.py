"""Small adapter around NetworkManager and the AP system services.

The service is deliberately injectable so it can be tested without a Raspberry Pi.
"""
import asyncio
import logging
import shutil

log = logging.getLogger(__name__)


class NetworkManager:
    def __init__(self, interface: str = "wlan0"):
        self.interface = interface

    async def connect(self, credentials: dict, timeout: int = 30) -> bool:
        if not shutil.which("nmcli"):
            log.warning("nmcli no esta instalado; no se puede validar WiFi")
            return False
        ssid = credentials["ssid"]
        password = credentials["password"]
        # hostapd controla wlan0 durante el AP. Al recibir credenciales,
        # devolvemos el control a NetworkManager para levantar la STA.
        managed = await asyncio.create_subprocess_exec(
            "nmcli", "device", "set", self.interface, "managed", "yes",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await managed.wait()
        command = ["nmcli", "device", "wifi", "connect", ssid, "password", password, "ifname", self.interface]
        if credentials.get("type") == "enterprise":
            command += ["802-1x.identity", credentials["username"]]
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            _, error = await asyncio.wait_for(process.communicate(), timeout)
        except asyncio.TimeoutError:
            process.kill()
            return False
        if process.returncode != 0:
            log.warning("No se pudo conectar a %s: %s", ssid, error.decode(errors="replace").strip())
        return process.returncode == 0

    async def stop_setup_ap(self) -> None:
        # Do not fail the whole gateway if one service is absent.
        for service in ("dnsmasq", "hostapd"):
            process = await asyncio.create_subprocess_exec("systemctl", "stop", service)
            await process.wait()
