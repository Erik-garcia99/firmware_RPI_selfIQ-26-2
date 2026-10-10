import json
import logging
from pathlib import Path
import threading
import time
from dataclasses import asdict
from .models import Credentials, atomic_json

log = logging.getLogger('selfiq')

class Controller:
    def __init__(self, config, network, clock=time.monotonic):
        self.c, self.net, self.clock = config, network, clock
        self.lock = threading.RLock()
        self.creds = None
        self.profile = None
        self.online = False
        self.busy = False
        self.error = ''
        self.last_request = 0
        self.last_retry = 0
        self.ap_wanted = True
        self.delivered = 0

    def touch(self):
        self.last_request = self.clock()

    def bootstrap(self):
        with self.lock:
            try:
                stored = json.loads(Path(self.c.state_file).read_text())
                if stored['profile'] not in ('selfiq-sta-a', 'selfiq-sta-b'):
                    raise ValueError('Perfil desconocido')
                self.creds = Credentials(**stored['credentials']).validate()
                self.profile = stored['profile']
            except FileNotFoundError:
                pass
            except (ValueError, KeyError, TypeError):
                self.error = 'Credenciales guardadas inválidas; configure de nuevo'
            self.net.stop_ap()
            if self.creds and not self.net.profile_exists(self.profile):
                try:
                    self.net.candidate(self.profile, self.creds)
                except RuntimeError:
                    self.error = 'No se pudo reconstruir el perfil guardado'
            self.online = bool(self.creds and self.net.activate(self.profile))
            self.ap_wanted = not self.online
            if self.ap_wanted:
                self.net.disconnect_sta()
                self.net.start_ap(False)
            self.touch()
            self.last_retry = self.clock()

    def configure(self, creds):
        with self.lock:
            old_creds, old_profile = self.creds, self.profile
            candidate = 'selfiq-sta-b' if self.profile == 'selfiq-sta-a' else 'selfiq-sta-a'
            try:
                self.online = False
                # One radio must release AP before scanning/associating with another channel.
                if self.c.virtual_ap:
                    self.net.stop_ap()
                self.net.candidate(candidate, creds)
                if not self.net.activate(candidate):
                    raise RuntimeError('No conectó: compruebe SSID, contraseña, señal y configuración Enterprise')
                atomic_json(self.c.state_file, {'profile': candidate, 'credentials': asdict(creds)})
                self.creds, self.profile, self.online = creds, candidate, True
                self.error = ''
                self.ap_wanted = True
                self.touch()
                # AP may fail even though STA was successfully verified and stored.
                self.net.start_ap(True)
            except Exception as e:
                log.warning('Configuración incompleta: %s', type(e).__name__)
                self.error = str(e) if isinstance(e, RuntimeError) else 'Error al guardar/configurar; consulte logs'
                if self.profile != candidate:
                    self.creds, self.profile = old_creds, old_profile
                    self.online = bool(old_creds and self.net.activate(old_profile))
                    if not self.online:
                        self.net.disconnect_sta()
                self.ap_wanted = True
                self.touch()
                try:
                    self.net.start_ap(self.online)
                except Exception:
                    self.error += '; AP no disponible: revise soporte AP+STA o use Wi-Fi USB'
                    log.exception('Fallo al restablecer AP')
            finally:
                self.busy = False
                self.last_retry = self.clock()

    def tick(self):
        with self.lock:
            if self.busy:
                return
            was_online = self.online
            self.online = bool(self.profile and self.net.active(self.profile))
            if was_online and not self.online:
                self.ap_wanted = True
                self.touch()
                if self.c.virtual_ap:
                    self.net.stop_ap()
            now = self.clock()
            if not self.online and self.creds and now - self.last_retry >= self.c.retry_seconds:
                self.last_retry = now
                if self.c.virtual_ap:
                    self.net.stop_ap()
                self.online = self.net.activate(self.profile)
                if not self.online:
                    self.net.disconnect_sta()
                else:
                    self.error = ''
                    self.touch()
            if self.online and self.ap_wanted and now - self.last_request >= self.c.idle_seconds:
                self.net.stop_ap()
                self.ap_wanted = False
            elif self.ap_wanted:
                self.net.start_ap(self.online)

    def reopen(self):
        with self.lock:
            self.ap_wanted = True
            self.touch()
            self.net.start_ap(self.online)

    def shutdown(self):
        with self.lock:
            self.net.stop_ap()

    def can_distribute(self):
        return bool(not self.busy and self.online and self.creds and self.net.ap_healthy())

    def status(self):
        return {'estado': 'conectando' if self.busy else ('conectado' if self.online else 'esperando_configuracion'),
                'ap_activo': self.net.ap_healthy(), 'ssid': self.creds.ssid if self.creds else None,
                'error': self.error, 'credenciales_entregadas': self.delivered,
                'segundos_para_cierre': max(0, int(self.c.idle_seconds - (self.clock() - self.last_request)))
                    if self.online and self.net.ap_healthy() else None}
