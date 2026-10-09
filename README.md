# Edge Gateway Raspberry Pi Zero 2 W — SelfIQ

## Provisioning inicial

### Instalación para producto terminado

Durante la preparación de la imagen de Raspberry Pi, copiar este proyecto a la
tarjeta y ejecutar una sola vez:

```bash
sudo ./install.sh
```

El instalador habilita `systemd`, `hostapd`, `dnsmasq` y `selfiq-rpi`. Después
de eso, el usuario final únicamente conecta la Raspberry a la energía: todos
los servicios arrancan automáticamente con el sistema, sin SSH ni comandos.

La primera funcionalidad está en `src/selfiq_rpi`: una página FastAPI para capturar WiFi normal o empresarial y un servidor TCP en `192.158.50.1:5000` que responde a `AB CD 02 01 00` con las credenciales en campos TLV (`0x69` SSID, `0x70` contraseña, `0x75` usuario). Las credenciales se guardan atómicamente en `/var/lib/selfiq/credentials.json`.

Instalar en la RPI:

```bash
sudo apt install -y hostapd dnsmasq python3 python3-pip
sudo apt install -y python3-fastapi python3-uvicorn python3-multipart
sudo python3 -m pip install --break-system-packages -r requirements.txt
sudo cp system/{hostapd.conf,dnsmasq.conf} /etc/
sudo install -m 755 system/selfiq-ap-network.sh /opt/selfiq-rpi/system/
sudo cp system/selfiq-ap-network.service /etc/systemd/system/
sudo cp system/selfiq-rpi.service /etc/systemd/system/
sudo ip addr add 192.158.50.1/24 dev wlan0
sudo systemctl daemon-reload && sudo systemctl enable --now selfiq-ap-network hostapd dnsmasq selfiq-rpi
SELFIQ_STATE_FILE=/var/lib/selfiq/credentials.json PYTHONPATH=src python3 -m selfiq_rpi.main
```

El puerto HTTP es 80 y el TCP de credenciales es 5000. Para la coexistencia AP+STA, `hostapd` y NetworkManager deben usar el mismo canal; validar el soporte del driver de la imagen Raspberry Pi. El firmware ESP32 actual tiene una inconsistencia al decodificar la respuesta ACK (lee una sola byte de acción/tipo), por lo que deberá corregirse para leer `ACTION` y `TYPE_FRAME` por separado antes de la integración en hardware.

Software del nodo **Edge Computing / Gateway IoT** del sistema SelfIQ.

Este repositorio contiene el software ejecutado en una **Raspberry Pi Zero 2 W**, encargada de actuar como punto central de comunicación entre los dispositivos IoT locales y los servicios en la nube.

La Raspberry Pi funciona como:

- broker MQTT local;
- gateway entre la red IoT y la nube;
- capa de procesamiento edge;
- almacenamiento temporal mediante SQLite;
- validador de mensajes;
- gestor de eventos pendientes;
- sincronizador con el backend cloud;
- punto de control de seguridad entre los ESP32 e Internet.

---

## Descripción

La arquitectura de SelfIQ utiliza un modelo híbrido Local/Nube.

Los dispositivos ESP32 no se comunican directamente con Internet.

Todos los módulos IoT se conectan mediante Wi-Fi a una red local y utilizan MQTT para intercambiar información con la Raspberry Pi Zero 2 W.

La Raspberry Pi recibe estos mensajes mediante Mosquitto, procesa y valida la información, almacena temporalmente los eventos y posteriormente los sincroniza con el backend desplegado en la nube.

Esto permite que parte del sistema continúe funcionando incluso cuando se pierde temporalmente la conexión a Internet.

---

# Función dentro de la arquitectura

La Raspberry Pi Zero 2 W es el punto central de la arquitectura IoT local.

```text
              ┌─────────────────────┐
              │     ESP32-CAM       │
              │ Monitoreo repisas   │
              └──────────┬──────────┘
                         │
                         │ MQTT
                         │
              ┌──────────▼──────────┐
              │                     │
              │ Raspberry Pi Zero   │
              │       2 W           │
              │                     │
              │ Mosquitto           │
              │ SQLite              │
              │ Edge Service        │
              │ Sync Service        │
              │                     │
              └───────┬─────┬──────┘
                      ▲     ▲
               MQTT   │     │ MQTT
                      │     │
       ┌──────────────┘     └──────────────┐
       │                                   │
┌──────┴─────────┐                 ┌───────┴──────────┐
│ ESP32 Aforo /  │                 │ ESP32 Guía      │
│ Ambiente       │                 │ LEDs            │
└────────────────┘                 └──────────────────┘

                         │
                         │ HTTPS / TLS
                         ▼

               ┌─────────────────────┐
               │    Backend Cloud    │
               │      FastAPI        │
               │      Railway        │
               └─────────────────────┘
