"""SelfIQ provisioning: STA is established once, AP alone is dynamically managed.

Never bring STA down, change an active STA profile, or force STA reconnection
in the periodic monitor. NetworkManager owns normal Wi-Fi after association.
"""
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
        self.external_wifi = False
        self.last_ap_attempt = 0

    def touch(self):
        with self.lock:
            self.last_request = self.clock()

    def _sta_connected(self):
        # Read-only; do not change the connection/profile on wlan0.
        name = self.net.run('nmcli', '-g', 'GENERAL.CONNECTION', 'device',
                            'show', self.c.sta_interface, optional=True)
        addr = self.net.run('nmcli', '-g', 'IP4.ADDRESS', 'device',
                            'show', self.c.sta_interface, optional=True)
        return bool(name and name != '--' and addr and addr != '--')

    def _sta_associated_profile(self):
        name = self.net.run('nmcli', '-g', 'GENERAL.CONNECTION', 'device',
                            'show', self.c.sta_interface, optional=True)
        return bool(name and name != '--')

    def _start_ap(self):
        if self.net.ap_healthy():
            return
        # Avoid restarting hostapd/dnsmasq every 5 s if this radio cannot do AP+STA.
        now = self.clock()
        if self.last_ap_attempt and now - self.last_ap_attempt < 30:
            return
        self.last_ap_attempt = now
        self.net.start_ap(self.online)

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

            # Respect ANY connection already active, including an OS-provided Wi-Fi.
            self.online = self._sta_connected()
            self.external_wifi = bool(self.online and not (self.profile and self.net.active(self.profile)))
            if not self.online and self.creds and self.profile:
                # Only at startup, when STA is not already connected.
                if not self.net.profile_exists(self.profile):
                    try:
                        self.net.candidate(self.profile, self.creds)
                    except RuntimeError:
                        self.error = 'No se pudo reconstruir el perfil guardado'
                if self.net.profile_exists(self.profile):
                    self.online = self.net.activate(self.profile)
                    if self.online:
                        self.net.enable_autoconnect(self.profile)
            self.ap_wanted = not self.online
            self.touch()
            if self.ap_wanted:
                self._start_ap()

    def configure(self, creds):
        with self.lock:
            # Never replace an existing SSH/network connection with the setup SSID.
            if self._sta_connected():
                raise RuntimeError('STA ya conectada: no se cambia su red activa')
            candidate = 'selfiq-sta-b' if self.profile == 'selfiq-sta-a' else 'selfiq-sta-a'
            try:
                # AP can be closed temporarily for single-radio association.
                if self.c.virtual_ap:
                    self.net.stop_ap()
                self.net.candidate(candidate, creds)
                if not self.net.activate(candidate):
                    raise RuntimeError('No conectó: compruebe SSID, contraseña y señal')
                atomic_json(self.c.state_file, {'profile': candidate, 'credentials': asdict(creds)})
                self.creds, self.profile, self.online = creds, candidate, True
                self.net.enable_autoconnect(candidate)
                self.external_wifi = False
                self.error = ''
                self.ap_wanted = True
                self.touch()
                # AP can be enabled for ESP provisioning; does not touch STA.
                self._start_ap()
            except Exception as e:
                self.error = str(e) if isinstance(e, RuntimeError) else 'Error al configurar; consulte logs'
                log.exception('Configuración Wi-Fi incompleta')
                self.online = self._sta_connected()
                self.ap_wanted = not self.online
                if self.ap_wanted:
                    try:
                        self._start_ap()
                    except Exception:
                        log.exception('No se pudo iniciar AP')
                raise
            finally:
                self.busy = False

    def tick(self):
        with self.lock:
            if self.busy:
                return
            # Never disturb an established STA connection, including one owned by OS.
            self.online = self._sta_connected()
            now = self.clock()
            if not self.online and not self._sta_associated_profile() and self.creds and self.profile:
                # Only restore SelfIQ's own saved profile when STA has no active link.
                # NM normally handles this; this is a bounded fallback, not a scan loop.
                if not self.last_retry or now - self.last_retry >= self.c.retry_seconds:
                    self.last_retry = now
                    if self.net.profile_exists(self.profile):
                        log.info('STA desconectada; intento de recuperación del perfil SelfIQ')
                        self.online = self.net.activate(self.profile)
            if self.ap_wanted and self.online and now - self.last_request >= self.c.idle_seconds:
                self.net.stop_ap()
                self.ap_wanted = False
            elif self.ap_wanted and not self.net.ap_healthy():
                self._start_ap()
            # No AP state changes merely because STA Wi-Fi briefly disconnects.

    def reopen(self):
        with self.lock:
            if not self._sta_connected() or not self.creds:
                raise ValueError('Wi-Fi o credenciales no disponibles para ESP32')
            self.online = True
            self.ap_wanted = True
            self.touch()
            self._start_ap()
            if not self.net.ap_healthy():
                raise RuntimeError('No se pudo habilitar AP')
            self.error = ''

    def stop_provisioning(self):
        with self.lock:
            self.ap_wanted = False
            self.net.stop_ap()

    def configuration_required(self):
        return not self.online

    def requested_credentials(self):
        with self.lock:
            if not self.can_distribute():
                return None
            self.touch()
            self.delivered += 1
            return self.creds

    def shutdown(self):
        with self.lock:
            self.net.stop_ap()

    def can_distribute(self):
        return bool(not self.busy and self.ap_wanted and self.online and self.creds and self.net.ap_healthy())

    def status(self):
        return {'estado': 'conectando' if self.busy else ('conectado' if self.online else 'esperando_configuracion'),
                'ap_activo': self.net.ap_healthy(), 'ssid': self.creds.ssid if self.creds else None,
                'configuracion_requerida': self.configuration_required(),
                'error': self.error, 'credenciales_entregadas': self.delivered,
                'segundos_para_cierre': max(0, int(self.c.idle_seconds - (self.clock() - self.last_request)))
                    if self.online and self.net.ap_healthy() else None}
