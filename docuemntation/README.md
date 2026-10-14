# SelfIQ · Raspberry Pi Zero 2 W

Implementación inicial de aprovisionamiento en Python/FastAPI, NetworkManager,
hostapd y dnsmasq. Servicio systemd al encender, sin intervención del comprador.
SSH puede mantenerse habilitado para desarrollo. Incluye registro del gateway
y recepción de órdenes mediante WebSocket saliente hacia el backend en Railway.
La configuración Wi-Fi y el TCP de ESP32 siguen en la Raspberry. La integración
MQTT y el reenvío de datos de sensores todavía no están implementados en este repo.
Consulta `CONFIGURAR_RAILWAY.md` para configurar el enlace cloud.

## Parámetros

| Parámetro | Valor |
|---|---|
| SSID de configuración | SelfIQ-Admi |
| Contraseña | $elfIQ-4dmi-1 |
| IP AP | 192.168.50.1/24 |
| HTTP | http://192.168.50.1 (puerto 80) |
| TCP ESP32 | 192.168.50.1:5000 |
| Máximo de estaciones AP | 8 |
| Cierre por inactividad | 300 segundos tras conectar STA |

192.158.50.1 era una IP pública; se cambia a 192.168.50.1.
Si la tienda ya usa 192.168.50.0/24 cambia ap_ip en config.json y RPI_IP_SETUP
al mismo rango privado libre. El programa rechaza solapar ese rango con STA.

## Flujo

1. Arranque: carga el último perfil verificado e intenta conectar con DHCP.
2. Sin configuración válida o si falla la conexión: levanta AP y DHCP. El
   usuario se conecta a SelfIQ-Admi y abre la URL indicada. No es un portal
   cautivo con apertura automática; la página no necesita Internet.
3. El usuario envía SSID/contraseña, o SSID/usuario/contraseña para Enterprise.
4. Comprueba conexión local e IP; no exige Internet ni valida el backend.
   Solo después guarda el perfil actual en /var/lib/selfiq/credentials.json.
5. Con STA verificada mantiene AP y TCP para entregar TLVs a las ESP32.
   Antes de verificar, las solicitudes reciben NACK; deben reintentar.
6. Cada solicitud válida entregada reinicia 300 s. Transcurrido ese tiempo,
   cierra clientes y servidor TCP, DHCP y AP; STA queda conectada. La página
   no mantiene abierto el AP por consultar estado. Un ACK confirma recepción
   del mensaje, no que la ESP ya conectó al Wi-Fi definitivo.
7. Sin credenciales verificadas, el AP no vence: el usuario aún debe configurar.
   Si STA cae, vuelve a AP y reintenta el perfil guardado cada 60 s.
8. En arranque posterior con credenciales que funcionan, entra directamente
   a STA sin AP. Se puede reabrir por endpoint administrativo.

## AP+STA con una radio

Modo por defecto: wlan0 para STA y uap0 virtual para AP. La misma radio trabaja
con ambos en un único canal 2.4 GHz. Durante la comprobación se baja el AP para
permitir escaneo/asociación STA y luego se abre en el canal de la conexión.
El teléfono y las ESP pueden tener que reconectar. Un cambio de canal del
router también requiere reiniciar el AP.

Este modo depende del firmware/controlador de la Zero 2 W y no se ha probado
físicamente aquí. No se garantiza AP+STA por el solo hecho de ejecutar Python.
Consulta `sudo bash tools/diagnose.sh`, especialmente `iw list`. Incluso si
anuncia una combinación compatible, debes ejecutar las pruebas físicas.
Si STA funciona pero AP+STA no, el programa conserva la conexión/credenciales
verificadas y deja un error; NO desconecta una STA válida para fingir éxito.

Alternativa con dos radios: wlan0 STA y un Wi-Fi USB que soporte AP en wlan1.
La instalación admite esa configuración, evitando compartir canal. Verifica
los nombres con iw dev; una Zero 2 W necesita conexión USB OTG adecuada.

## Preparar e instalar (lo hace el desarrollador antes de entregar)

Destinado a Raspberry Pi OS Lite Bookworm o posterior con NetworkManager.
No mezcles este servicio con una instalación antigua de dhcpcd/RaspAP u otros
servicios que controlen las mismas interfaces. Se asume una Raspberry dedicada
al proyecto. El instalador detiene los servicios globales hostapd/dnsmasq para
usar instancias propias; no borra perfiles ajenos de NetworkManager.
Requiere Internet para descargar apt/pip. No se ejecutó en tu Raspberry.

Configura país Wi-Fi, usuario y SSH al grabar la imagen con Raspberry Pi Imager.
Para entregar el producto, no pongas Wi-Fi del cliente en Imager; instala y
prueba esta aplicación antes, usando Ethernet USB o una red de desarrollo.
Con SSH habilitado para pruebas puedes entrar al AP o a la IP STA usando
credenciales SSH propias. El SSID/AP no crea una cuenta SSH ni habilita acceso
root por SSH. No hay pasos de SSH para el comprador.

```bash
tar -xzf SelfIQ_RPI.tar.gz
cd SelfIQ_RPI
sudo bash install.sh
```

El inicio puede cortar la sesión SSH por Wi-Fi. Usa consola local o Ethernet
para la primera instalación. Por defecto país MX; configura otro si corresponde:
```bash
sudo WIFI_COUNTRY=MX bash install.sh
```
Dos radios, en instalación nueva:
```bash
sudo STA_IFACE=wlan0 AP_IFACE=wlan1 VIRTUAL_AP=false bash install.sh
```
Si ya existe /etc/selfiq/config.json, el instalador conserva ese archivo, token
y nombres de interfaces. Para cambiar de modo edítalo y vuelve a ejecutar el
instalador para sincronizar la regla unmanaged de NetworkManager. Cambios
normales de CA/tiempos solo necesitan reiniciar el servicio.

```bash
sudo systemctl status selfiq --no-pager
sudo journalctl -u selfiq -f
sudo systemctl restart selfiq
```

## Redes empresariales

Implementado WPA2 Enterprise PEAP/MSCHAPv2 con usuario/contraseña. No es soporte
universal de toda red corporativa. Obtén del administrador la CA y el dominio
del RADIUS; no se inventan parámetros para UABC. Instala CA si usa una privada,
y ajusta /etc/selfiq/config.json, por ejemplo:
```json
"enterprise_ca": "/etc/selfiq/empresa-ca.pem",
"enterprise_domain": "radius.empresa.example"
```
Estos son ejemplos, no los datos de una red real. El archivo CA debe ser legible
por root. Si usa CA pública puede bastar /etc/ssl/certs/ca-certificates.crt con
el dominio correcto. Reinicia selfiq. El formulario solo pide los tres campos
que solicitaste; bloquea Enterprise hasta preparar la configuración del RADIUS.
La ESP también necesita la CA/configuración EAP en su firmware: el protocolo
actual no transmite esos parámetros. EAP-TLS, portales cautivos, políticas de
registro MAC o redes exclusivamente 5 GHz quedan fuera de este primer alcance.

## Protocolo binario

| Campo | Tamaño | Significado |
|---|---|---|
| HEADER | 2 bytes, big endian | ABCD petición; 5433 ACK/respuesta/NACK |
| LEN | 1 byte | Bytes restantes: byte tipo/acción + DATA |
| TYPE/ACTION | 1 byte | (TYPE_FRAME << 4) \| ACTION |
| DATA | LEN-1 bytes | Payload o TLVs |

NACK es la excepción fija `54 33 FF FF 00`, no 255 bytes de contenido.
Se conserva cómo tu ESP32 realmente genera NACK: usa header ACK, no CAFE.
Petición: `AB CD 02 01 00` (tipo 0, acción 1).
Respuesta Personal: `54 33 LEN 12 [69 len SSID] [70 len contraseña]`.
Respuesta Enterprise: `54 33 LEN 22 [69 len SSID] [75 len usuario] [70 len contraseña]`.
ACK de recepción ESP: `54 33 02 30 01`.
IDs/longitudes binarios; strings UTF-8 sin terminador NUL.
SSID máximo 32 bytes; Personal password 8–63; Enterprise user 1–31 y password
1–64. DATA máxima 160; LEN máxima usada por parser 161. No serialices el struct
C directamente: el padding y los bitfields no son el formato de red.

El firmware ESP32 debe utilizar este contrato binario. El nuevo paquete MQTT
de ESP32 también necesita una respuesta TCP para broker y un puente de registro
MQTT, que aún no están implementados por este cambio al enlace cloud.

## Reabrir AP (desarrollo / futura app)

POST /admin/ap/enable requiere X-SelfIQ-Token. El token se genera al instalar y
queda en /etc/selfiq/config.json (root, modo 600). No se publica en la página.
Desde una sesión SSH de desarrollo en la Raspberry:
```bash
sudo /opt/selfiq/.venv/bin/python - <<'PY'
import json, urllib.request
c = json.load(open('/etc/selfiq/config.json'))
r = urllib.request.Request('http://127.0.0.1/admin/ap/enable', data=b'',
    headers={'X-SelfIQ-Token': c['admin_token']}, method='POST')
print(urllib.request.urlopen(r).read().decode())
PY
```
La app puede solicitar órdenes al backend, que las entrega por el canal WSS
saliente de esta Raspberry. Para acceso administrativo directo en desarrollo
usa un túnel SSH;
no publiques el HTTP administrativo en Internet. Esta versión no tiene TLS.
La página funciona solo para clientes AP/localhost; desde LAN exige el token.
No activa NAT: el AP sirve para configuración, no para compartir Internet.

## Pruebas

En tu computadora (sin tocar interfaces de red):
```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt httpx
.venv/bin/python -m unittest discover -s tests -v
```
Se comprobaron 36 pruebas (incluido WSS con certificado verificado, registro
firmado, reconexión y configuración de Railway): longitudes/TLVs, fragmentación y concatenación TCP,
NACK, validación y almacenamiento atómico, temporizador, reconexión, fallos
simulados de AP/STA, HTTP, protección de origen/token y cierre real de sockets.
Las pruebas de red Wi-Fi usan un adaptador simulado; no acreditan brcmfmac,
DHCP ni PEAP en hardware. Sintaxis Python y Bash revisadas.

Prueba física antes de entregar:
1. Arranca sin credenciales: teléfono ve AP y obtiene IP .10–.200; abre página.
2. Envía contraseña incorrecta: vuelve AP; no se reemplaza el perfil válido.
3. Envía contraseña correcta: obtiene IP STA y reaparece AP con TCP.
4. En una computadora conectada al AP ejecuta:
   `python3 tools/esp_simulator.py --fragment` (no imprime contraseña).
5. Integra los cambios ESP y prueba WPA2/Enterprise reales. Verifica NVS.
6. Espera 5 min sin solicitudes: desaparece AP y TCP, STA/SSH siguen activos.
7. Reinicia: usa credenciales sin AP. Apaga el router: AP vuelve.
8. Prueba /admin/ap/enable. Comprueba que un cliente LAN sin token recibe 403.

Archivos de estado: /var/lib/selfiq/credentials.json y perfiles NetworkManager
selfiq-sta-a/b, accesibles a root. El slot inactivo puede conservar un perfil
anterior/de prueba, pero no se selecciona al arranque. No se registran
contraseñas en HTTP/TCP. La contraseña fija del AP es compartida entre productos:
para una versión comercial conviene emparejamiento y clave propia por equipo.

Referencias de configuración:
- https://www.raspberrypi.com/documentation/computers/configuration.html
- https://networkmanager.dev/docs/api/latest/nm-settings-nmcli.html
- https://docs.raspap.com/features-experimental/ap-sta/
