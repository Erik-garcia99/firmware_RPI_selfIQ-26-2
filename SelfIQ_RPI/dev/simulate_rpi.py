"""SIMULACIÓN: no cambia redes del host, no genera un AP Wi-Fi físico."""
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import uvicorn
from selfiq.app import create_app
from selfiq.models import Config, Credentials, atomic_json
from dataclasses import asdict


class SimulatedNetwork:
    def __init__(self):
        self.current = None
        self.ap = False
        self.profiles = set()

    def stop_ap(self): self.ap = False
    def start_ap(self, connected=False): self.ap = True
    def ap_healthy(self): return self.ap
    def active(self, name): return self.current == name
    def profile_exists(self, name): return name in self.profiles
    def activate(self, name):
        self.current = name
        return True
    def candidate(self, name, creds): self.profiles.add(name)
    def disconnect_sta(self): self.current = None


def main():
    with tempfile.TemporaryDirectory() as directory:
        config = Config(state_file=directory + '/credentials.json', admin_token='SIMULATION_ONLY_' * 3,
                        backend_ws_url=os.environ['SELFIQ_BACKEND_WS_URL'],
                        gateway_id=os.environ['SELFIQ_GATEWAY_ID'], gateway_token=os.environ['SELFIQ_GATEWAY_TOKEN'])
        # Deliberately fictitious Wi-Fi credentials for the simulated TCP response.
        atomic_json(config.state_file, {'profile': 'selfiq-sta-a',
                    'credentials': asdict(Credentials('WIFI_SIMULADO', 'NO_ES_WIFI_REAL'))})
        config = replace(config, ap_ip='0.0.0.0')
        uvicorn.run(create_app(config, SimulatedNetwork()), host='0.0.0.0', port=8081,
                    proxy_headers=False, access_log=False)


if __name__ == '__main__':
    main()
