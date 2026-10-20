"""NetworkManager owns STA. hostapd + dnsmasq own only the setup AP."""
from pathlib import Path
import ipaddress
import os
import re
import subprocess
import time

class NetworkError(RuntimeError):
    pass

class Network:
    def __init__(self, config):
        self.c = config
        self.processes = []
        self.ap_active = False
        self.ap_channel = None
        self.runtime = Path(config.runtime_dir)
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)

    def run(self, *args, optional=False, timeout=15):
        # Never log argv/stdout on failure: nmcli arguments may contain passwords.
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                               env={**os.environ, 'LC_ALL': 'C'})
        except (OSError, subprocess.TimeoutExpired) as e:
            if optional:
                return ''
            raise NetworkError('Herramienta de red ausente o agotó su tiempo') from e
        if p.returncode and not optional:
            raise NetworkError('Falló la configuración de red; consulte NetworkManager/hostapd')
        return p.stdout.strip() if p.returncode == 0 else ''

    def profile_exists(self, name):
        return bool(self.run('nmcli', '-g', 'connection.uuid', 'connection', 'show', name, optional=True))

    def active(self, name):
        current = self.run('nmcli', '-g', 'GENERAL.CONNECTION', 'device', 'show',
                           self.c.sta_interface, optional=True)
        ip = self.run('nmcli', '-g', 'IP4.ADDRESS', 'device', 'show',
                      self.c.sta_interface, optional=True)
        return current == name and bool(ip) and ip != '--'

    def activate(self, name):
        try:
            self.run('nmcli', '--wait', str(self.c.connect_timeout), 'connection', 'up',
                     name, 'ifname', self.c.sta_interface, timeout=self.c.connect_timeout + 5)
            return self.active(name)
        except NetworkError:
            return False

    def channel(self):
        info = self.run('iw', 'dev', self.c.sta_interface, 'info')
        match = re.search(r'channel (\d+)', info)
        if not match:
            raise NetworkError('No se pudo consultar canal STA')
        channel = int(match[1])
        if not 1 <= channel <= 14:
            raise NetworkError('El AP requiere canal 2.4 GHz')
        return channel

    def candidate(self, name, creds):
        # Solo administrar perfiles propios de SelfIQ; nunca borrar la red existente.
        if self.profile_exists(name):
            raise NetworkError('Perfil SelfIQ ya existente; no se reemplazará automáticamente')
        if creds.kind != 1:
            raise NetworkError('Solo se admite Wi-Fi Personal (WPA2-PSK)')
        args = ['nmcli', 'connection', 'add', 'type', 'wifi', 'ifname', self.c.sta_interface,
                'con-name', name, 'ssid', creds.ssid, 'connection.autoconnect', 'no',
                'ipv4.method', 'auto', 'ipv6.method', 'disabled',
                '802-11-wireless.band', 'bg',
                'wifi-sec.key-mgmt', 'wpa-psk', 'wifi-sec.psk', creds.password]
        self.run(*args)

    def ap_healthy(self):
        return self.ap_active and len(self.processes) == 2 and all(p.poll() is None for p in self.processes)

    def start_ap(self, connected=False):
        if self.ap_healthy():
            channel = self.channel() if connected and self.c.virtual_ap else self.c.setup_channel
            if self.ap_channel == channel:
                return
        self.stop_ap()
        # Refuse overlapping STA subnet instead of replacing a route to the store network.
        addresses = self.run('ip', '-o', '-4', 'addr', 'show', 'dev', self.c.sta_interface)
        subnet = ipaddress.ip_network(self.c.ap_ip + '/24', strict=False)
        for cidr in re.findall(r'inet (\S+)', addresses):
            if subnet.overlaps(ipaddress.ip_interface(cidr).network):
                raise NetworkError('La subred STA coincide con la subred de configuración; cambie ap_ip y la ESP32')
        channel = self.channel() if connected and self.c.virtual_ap else self.c.setup_channel
        try:
            if self.c.virtual_ap:
                if not Path('/sys/class/net/' + self.c.ap_interface).exists():
                    self.run('iw', 'dev', self.c.sta_interface, 'interface', 'add', self.c.ap_interface, 'type', '__ap')
                # Distinct locally administered MAC; stable relative to STA.
                mac = bytearray.fromhex(Path('/sys/class/net/' + self.c.sta_interface + '/address').read_text().strip().replace(':', ''))
                mac[0] = (mac[0] | 2) & 254
                mac[-1] ^= 1
                self.run('ip', 'link', 'set', 'dev', self.c.ap_interface, 'down')
                self.run('ip', 'link', 'set', 'dev', self.c.ap_interface, 'address', ':'.join(f'{x:02x}' for x in mac))
            self.run('nmcli', 'device', 'set', self.c.ap_interface, 'managed', 'no')
            self.run('ip', 'addr', 'replace', self.c.ap_ip + '/24', 'dev', self.c.ap_interface)
            self.run('ip', 'link', 'set', 'dev', self.c.ap_interface, 'up')
            hostapd = self.runtime / 'hostapd.conf'
            ssid = self.c.ap_ssid.encode().hex()
            # Hex SSID and PBKDF2 PSK avoid configuration-file injection.
            import hashlib
            psk = hashlib.pbkdf2_hmac('sha1', self.c.ap_password.encode(), self.c.ap_ssid.encode(), 4096, 32).hex()
            hostapd.write_text(f'interface={self.c.ap_interface}\ndriver=nl80211\nssid2={ssid}\n'
                              f'country_code={self.c.country}\nhw_mode=g\nchannel={channel}\n'
                              f'wpa=2\nwpa_key_mgmt=WPA-PSK\nrsn_pairwise=CCMP\nwpa_psk={psk}\n'
                              f'ctrl_interface={self.runtime}/hostapd\n'
                              'auth_algs=1\nmax_num_sta=8\nap_isolate=1\n')
            hostapd.chmod(0o600)
            prefix = self.c.ap_ip.rsplit('.', 1)[0]
            dns = self.runtime / 'dnsmasq.conf'
            dns.write_text(f'interface={self.c.ap_interface}\nbind-interfaces\nport=0\n'
                           f'dhcp-range={prefix}.10,{prefix}.200,255.255.255.0,1h\n'
                           f'dhcp-option=3,{self.c.ap_ip}\ndhcp-option=6\n'
                           f'dhcp-leasefile={self.runtime}/leases\n')
            self.processes.append(subprocess.Popen(['hostapd', str(hostapd)]))
            self.processes.append(subprocess.Popen(['dnsmasq', '--keep-in-foreground', '--conf-file=' + str(dns)]))
            for _ in range(16):
                if not all(p.poll() is None for p in self.processes):
                    raise NetworkError('No inició AP/DHCP; revise logs y soporte AP+STA del controlador')
                status = self.run('hostapd_cli', '-p', str(self.runtime / 'hostapd'), '-i', self.c.ap_interface, 'status', optional=True)
                if 'state=ENABLED' in status:
                    break
                time.sleep(0.5)
            else:
                raise NetworkError('AP no llegó al estado ENABLED; revise firmware/controlador')
            self.ap_active = True
            self.ap_channel = channel
        except Exception:
            self.stop_ap()
            raise

    def stop_ap(self):
        for p in self.processes:
            if p.poll() is None:
                p.terminate()
        for p in self.processes:
            try:
                p.wait(timeout=4)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        self.processes = []
        self.ap_active = False
        self.ap_channel = None
        if Path('/sys/class/net/' + self.c.ap_interface).exists():
            self.run('ip', 'addr', 'del', self.c.ap_ip + '/24', 'dev', self.c.ap_interface, optional=True)
            self.run('ip', 'link', 'set', 'dev', self.c.ap_interface, 'down', optional=True)
            if self.c.virtual_ap:
                self.run('iw', 'dev', self.c.ap_interface, 'del', optional=True)

    def disconnect_sta(self):
        # No desconectar wlan0: puede tener un perfil del usuario ajeno a SelfIQ.
        pass
