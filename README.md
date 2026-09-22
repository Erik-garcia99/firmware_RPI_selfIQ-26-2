# Edge Gateway Raspberry Pi Zero 2 W — SelfIQ

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
