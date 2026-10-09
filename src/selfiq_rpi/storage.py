import json
from pathlib import Path
from threading import Lock


class CredentialStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = Lock()

    def load(self) -> dict | None:
        try:
            with self.path.open() as file:
                value = json.load(file)
            return value if value.get("ssid") and value.get("password") else None
        except (FileNotFoundError, OSError, ValueError, AttributeError):
            return None

    def save(self, credentials: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        with self._lock:
            temporary.write_text(json.dumps(credentials, ensure_ascii=False), encoding="utf-8")
            temporary.replace(self.path)
