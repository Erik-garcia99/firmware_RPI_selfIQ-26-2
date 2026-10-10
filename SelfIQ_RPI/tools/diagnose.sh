#!/usr/bin/env bash
set -u
echo 'Interfaces inalámbricas:'
iw dev
echo 'Combinaciones que anuncia el controlador (AP+managed y #channels):'
iw list
echo 'Estado NetworkManager:'
nmcli device status
echo 'Estado radio:'
rfkill list
echo 'Servicio SelfIQ:'
systemctl status selfiq --no-pager
echo 'Últimos logs (no incluyen credenciales TCP):'
journalctl -u selfiq -n 60 --no-pager
