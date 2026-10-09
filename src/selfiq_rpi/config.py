from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    ap_ssid: str = os.getenv("SELFIQ_AP_SSID", "SelfIQ-Admi")
    ap_password: str = os.getenv("SELFIQ_AP_PASSWORD", "$elfIQ-4dmi-1")
    ap_ip: str = os.getenv("SELFIQ_AP_IP", "192.158.50.1")
    tcp_port: int = int(os.getenv("SELFIQ_TCP_PORT", "5000"))
    http_host: str = os.getenv("SELFIQ_HTTP_HOST", "0.0.0.0")
    http_port: int = int(os.getenv("SELFIQ_HTTP_PORT", "80"))
    state_file: Path = Path(os.getenv("SELFIQ_STATE_FILE", "/var/lib/selfiq/credentials.json"))
    ap_idle_seconds: int = int(os.getenv("SELFIQ_AP_IDLE_SECONDS", "300"))
    interface: str = os.getenv("SELFIQ_WIFI_INTERFACE", "wlan0")


settings = Settings()
