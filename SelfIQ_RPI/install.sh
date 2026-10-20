#!/usr/bin/env bash
set -euo pipefail
if [[ $EUID -ne 0 ]]; then echo 'Ejecute: sudo bash install.sh'; exit 1; fi
task_source_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
sta_iface="${STA_IFACE:-wlan0}"
ap_iface="${AP_IFACE:-uap0}"
virtual_ap="${VIRTUAL_AP:-true}"
wifi_country="${WIFI_COUNTRY:-MX}"
if [[ "$virtual_ap" != true && "$virtual_ap" != false ]]; then echo 'VIRTUAL_AP debe ser true o false'; exit 1; fi
if [[ "$sta_iface" == "$ap_iface" ]]; then echo 'STA y AP requieren interfaces distintas'; exit 1; fi
if [[ ! "$sta_iface" =~ ^[a-zA-Z0-9_-]{1,15}$ || ! "$ap_iface" =~ ^[a-zA-Z0-9_-]{1,15}$ || ! "$wifi_country" =~ ^[A-Z]{2}$ ]]; then echo 'Interfaz o país inválidos'; exit 1; fi
echo 'Instalando SelfIQ sin desconectar ni sobrescribir la Wi-Fi existente.'
apt-get update
apt-get install -y python3-venv network-manager iw hostapd dnsmasq iproute2 rfkill curl ca-certificates avahi-daemon avahi-utils
# Only announce the gateway hostname; TCP remains bound to the AP.
# Remove the former misleading service record if it was installed.
rm -f /etc/avahi/services/selfiq-broker.service
# Avahi host-name is independent of the system's Linux hostname.
if grep -qE '^#?host-name=' /etc/avahi/avahi-daemon.conf; then
  sed -i -E 's/^#?host-name=.*/host-name=selfiq-gateway/' /etc/avahi/avahi-daemon.conf
else
  sed -i '/^\[server\]/a host-name=selfiq-gateway' /etc/avahi/avahi-daemon.conf
fi
systemctl enable --now avahi-daemon.service
systemctl restart avahi-daemon.service
# SelfIQ launches its own isolated hostapd/dnsmasq processes; avoid double listeners.
systemctl disable hostapd.service dnsmasq.service
systemctl enable NetworkManager.service
install -d -m 755 /opt/selfiq
install -d -m 700 /etc/selfiq /var/lib/selfiq
cp -r "$task_source_dir/selfiq" /opt/selfiq/
install -d -m 755 /opt/selfiq/tools
install -m 644 "$task_source_dir/tools/configure_backend.py" /opt/selfiq/tools/configure_backend.py
install -m 644 "$task_source_dir/requirements.txt" /opt/selfiq/requirements.txt
python3 -m venv /opt/selfiq/.venv
/opt/selfiq/.venv/bin/pip install -r /opt/selfiq/requirements.txt
if [[ ! -f /etc/selfiq/config.json ]]; then
  STA_IFACE="$sta_iface" AP_IFACE="$ap_iface" VIRTUAL_AP="$virtual_ap" WIFI_COUNTRY="$wifi_country" /opt/selfiq/.venv/bin/python - <<'PY'
import os, secrets, sys
sys.path.insert(0, '/opt/selfiq')
from selfiq.models import Config, atomic_json
from dataclasses import asdict
c = Config(sta_interface=os.environ['STA_IFACE'], ap_interface=os.environ['AP_IFACE'],
           virtual_ap=os.environ['VIRTUAL_AP']=='true', country=os.environ['WIFI_COUNTRY'],
           admin_token=secrets.token_urlsafe(32))
atomic_json('/etc/selfiq/config.json', asdict(c))
PY
fi
# Create configuration guidance once; never overwrite a deployed URL/token.
if [[ ! -f /etc/selfiq/backend.env ]]; then
  install -m 600 /dev/null /etc/selfiq/backend.env
fi
# Config is authoritative on reinstallation; do not silently alter interface mapping/token.
configured_ap="$(/opt/selfiq/.venv/bin/python -c 'import json; print(json.load(open("/etc/selfiq/config.json"))["ap_interface"])')"
if [[ ! "$configured_ap" =~ ^[a-zA-Z0-9_-]{1,15}$ ]]; then echo 'Interfaz inválida en config.json'; exit 1; fi
install -d /etc/NetworkManager/conf.d
cat > /etc/NetworkManager/conf.d/90-selfiq-ap.conf <<EOF
[keyfile]
unmanaged-devices=interface-name:$configured_ap
EOF
install -m 644 "$task_source_dir/selfiq.service" /etc/systemd/system/selfiq.service
systemctl daemon-reload
systemctl enable selfiq.service
# La instalación no inicia ni reinicia SelfIQ, para proteger la conexión remota.
echo 'Servicio instalado, pero no iniciado. Para iniciarlo: sudo systemctl start selfiq.service'
echo 'Instalado. Estado: sudo systemctl status selfiq --no-pager'
echo 'Página: conecte el teléfono a SelfIQ-Admi y abra http://192.168.50.1'
echo 'Backend Railway: edite sudo nano /etc/selfiq/backend.env y reinicie selfiq.'
echo 'Token de sucursal: sudo /opt/selfiq/.venv/bin/python /opt/selfiq/tools/configure_backend.py'
