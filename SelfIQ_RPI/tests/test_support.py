"""Complete, isolated test settings; never use installation paths or credentials."""
import atexit
import tempfile
from pathlib import Path

from selfiq.models import Config


_ROOT = tempfile.TemporaryDirectory(prefix='selfiq-tests-')
atexit.register(_ROOT.cleanup)


def make_config(**overrides):
    root = Path(tempfile.mkdtemp(dir=_ROOT.name))
    settings = dict(
        sta_interface='wlan0', ap_interface='uap0', virtual_ap=True,
        ap_ip='192.168.50.1', ap_ssid='SelfIQ-Test', ap_password='test-password',
        country='MX', setup_channel=6, idle_seconds=300, connect_timeout=30,
        retry_seconds=60, recovery_rounds=4, recovery_attempts=5,
        recovery_retry_seconds=300, offline_ap_delay=30,
        offline_db=str(root / 'outbox.sqlite3'), offline_max_fraction=.5,
        offline_reserve_fraction=.01, offline_reserve_bytes=0,
        offline_max_payload=4096, admin_token='A' * 32,
        state_file=str(root / 'credentials.json'), runtime_dir=str(root / 'run'),
        tcp_port=5000, backend_ws_url='', gateway_id='', gateway_token='',
        gateway_state_file=str(root / 'gateway.json'),
        influx_url='', influx_bucket='', influx_org='', influx_token='',
    )
    settings.update(overrides)
    return Config(**settings)
