"""SelfIQ provisioning: STA is established once, AP alone is dynamically managed.

Never bring STA down, change an active STA profile, or force STA reconnection
in the periodic monitor. NetworkManager owns normal Wi-Fi after association.
"""
import logging
import threading
import time
from dataclasses import asdict
from .models import Credentials
from .credential_integrity import read_state, write_state

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
        self.recovery_ap = False
        self.offline_since = None
        self.recovery_count = 0
        self.empty_ap_since = None

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
                stored = read_state(self.c.state_file)
                if stored['profile'] not in ('selfiq-sta-a', 'selfiq-sta-b'):
                    raise ValueError('Perfil desconocido')
                self.creds = Credentials(**stored['credentials']).validate()
                self.profile = stored['profile']
                write_state(self.c.state_file, stored)  # migrate validated legacy JSON
            except FileNotFoundError:
                pass
            except (ValueError, KeyError, TypeError, OSError):
                self.creds = self.profile = None
                self.error = 'Credenciales guardadas invalidas/no accesibles; configure de nuevo'
                log.error('[INTEGRITY] CREDENTIAL_STATE_INVALID')

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
            self.recovery_ap = not self.online
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
                write_state(self.c.state_file, {'profile': candidate, 'credentials': asdict(creds)})
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
            self.online = self._sta_connected()
            now = self.clock()
            if not self.online:
                if self.offline_since is None:
                    self.offline_since = now
                    log.warning('[NETWORK] WIFI_LOST; preservando credenciales')
                if now - self.offline_since >= self.c.offline_ap_delay:
                    self.ap_wanted = self.recovery_ap = True
                    self._start_ap()
                limit = self.c.recovery_rounds * self.c.recovery_attempts
                interval = (self.c.retry_seconds if self.recovery_count < limit
                            else self.c.recovery_retry_seconds)
                if (not self._sta_associated_profile() and self.profile and self.creds
                        and (not self.last_retry or now - self.last_retry >= interval)):
                    self.last_retry = now
                    self.recovery_count += 1
                    log.warning('[NETWORK] WIFI_RETRY %d (presupuesto inicial %d)',
                                self.recovery_count, limit)
                    if self.net.profile_exists(self.profile):
                        self.online = self.net.activate(self.profile)
                    if not self.online and self.recovery_count == limit:
                        self.error = 'WIFI_RETRY_EXHAUSTED'
                        log.error('[NETWORK] WIFI_RETRY_EXHAUSTED; AP local, reintento cada %ds',
                                  self.c.recovery_retry_seconds)
                # External OS profiles remain managed by NetworkManager.
            if self.online:
                if self.offline_since is not None:
                    log.info('[NETWORK] WIFI_RECOVERED; esperando salida de clientes del AP')
                self.offline_since = None
                self.recovery_count = 0
                if self.error == 'WIFI_RETRY_EXHAUSTED':
                    self.error = ''
                if self.recovery_ap:
                    # With one radio AP must follow the channel of the recovered STA.
                    self.net.start_ap(connected=True)
                    clients = self.net.ap_clients()
                    if clients is None or clients > 0:
                        self.empty_ap_since = None
                    elif self.empty_ap_since is None:
                        self.empty_ap_since = now
                    elif now - self.empty_ap_since >= self.c.idle_seconds:
                        self.recovery_ap = self.ap_wanted = False
                        self.net.stop_ap()
                elif self.ap_wanted and now - self.last_request >= self.c.idle_seconds:
                    self.net.stop_ap()
                    self.ap_wanted = False
            if self.ap_wanted and not self.net.ap_healthy():
                self._start_ap()

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
            if self.recovery_ap:
                raise ValueError('AP en recuperacion; espere a que salgan los clientes')
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
        return {'recovery_ap': self.recovery_ap, 'wifi_connected': self.online,
                'recovery_attempts': self.recovery_count, 'estado': 'conectando' if self.busy else ('conectado' if self.online else 'esperando_configuracion'),
                'ap_activo': self.net.ap_healthy(), 'ssid': self.creds.ssid if self.creds else None,
                'configuracion_requerida': self.configuration_required(),
                'error': self.error, 'credenciales_entregadas': self.delivered,
                'segundos_para_cierre': max(0, int(self.c.idle_seconds - (self.clock() - self.last_request)))
                    if self.online and self.net.ap_healthy() else None}
