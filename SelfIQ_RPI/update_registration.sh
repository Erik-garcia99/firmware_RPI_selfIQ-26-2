#!/usr/bin/env bash
# Ejecutar desde el proyecto RPi completo, despues de aplicar el parche.
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo 'Ejecute: sudo bash update_registration.sh'; exit 1; }
task_source_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
[[ "$task_source_dir" != /opt/selfiq ]] || { echo 'Ejecute desde su carpeta de proyecto, fuera de /opt/selfiq.'; exit 1; }
for name in __init__.py app.py models.py network.py controller.py protocol.py backend_link.py backend_config.py gateway_identity.py mqtt_bridge.py index.html; do
  [[ -f "$task_source_dir/selfiq/$name" ]] || { echo "Falta selfiq/$name. Aplique el parche sobre el proyecto RPi completo."; exit 1; }
done
[[ -x /opt/selfiq/.venv/bin/python ]] || { echo 'Falta la instalacion existente en /opt/selfiq.'; exit 1; }
# Solo agrega la dependencia del puente; no reinstala ni reconfigura Wi-Fi/Mosquitto.
/opt/selfiq/.venv/bin/python -m pip install 'paho-mqtt>=2,<3'
(cd "$task_source_dir" && /opt/selfiq/.venv/bin/python -c 'from selfiq.app import create_app; from selfiq.mqtt_bridge import MQTTBridge')
task_backup="$(mktemp -d /opt/selfiq-backup-registro-XXXXXXXX)"
cp -a /opt/selfiq/selfiq "$task_backup/"
echo "Respaldo de codigo: $task_backup/selfiq"
systemctl stop selfiq.service
rollback() {
  echo 'Actualizacion fallida; restaurando codigo anterior.' >&2
  cp -a "$task_backup/selfiq/." /opt/selfiq/selfiq/
  systemctl restart selfiq.service || true
}
trap rollback ERR
cp -a "$task_source_dir/selfiq/." /opt/selfiq/selfiq/
(cd /opt/selfiq && /opt/selfiq/.venv/bin/python -c 'from selfiq.app import create_app; from selfiq.mqtt_bridge import MQTTBridge; print("Puente MQTT instalado")')
systemctl restart selfiq.service
systemctl is-active --quiet selfiq.service
trap - ERR
journalctl -u selfiq.service -n 30 --no-pager
echo 'Observe la conexion y el alta: sudo journalctl -u selfiq.service -f'
