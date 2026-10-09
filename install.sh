#!/bin/bash
# Instala SelfIQ como servicio del sistema. Ejecutar una sola vez al preparar
# la tarjeta SD; el usuario final no necesita SSH ni ejecutar este script.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Ejecuta este instalador como root: sudo ./install.sh" >&2
  exit 1
fi

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
INSTALL_DIR=/opt/selfiq-rpi

apt-get update
apt-get install -y hostapd dnsmasq python3 python3-pip iproute2
python3 -m pip install --break-system-packages -r "$ROOT_DIR/requirements.txt"

mkdir -p "$INSTALL_DIR" /var/lib/selfiq
cp -a "$ROOT_DIR/src" "$INSTALL_DIR/"
cp "$ROOT_DIR/requirements.txt" "$INSTALL_DIR/"
install -m 0644 "$ROOT_DIR/system/hostapd.conf" /etc/hostapd/hostapd.conf
install -m 0644 "$ROOT_DIR/system/dnsmasq.conf" /etc/dnsmasq.conf
install -m 0755 "$ROOT_DIR/system/selfiq-ap-network.sh" "$INSTALL_DIR/system-selfiq-ap-network.sh"
install -m 0644 "$ROOT_DIR/system/selfiq-ap-network.service" /etc/systemd/system/
install -m 0644 "$ROOT_DIR/system/selfiq-rpi.service" /etc/systemd/system/

# Debian/Raspberry Pi OS suele entregar hostapd enmascarado por defecto.
systemctl unmask hostapd || true
systemctl unmask dnsmasq || true
systemctl daemon-reload
systemctl enable selfiq-ap-network.service hostapd.service dnsmasq.service selfiq-rpi.service
systemctl restart selfiq-ap-network.service
systemctl restart hostapd.service dnsmasq.service selfiq-rpi.service

echo "SelfIQ instalado: el AP y sus servicios arrancaran automaticamente al encender."
