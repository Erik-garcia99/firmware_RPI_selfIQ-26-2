"""Payload MQTT: comando + destino NVS; respuesta añade resultado.

Valores compartidos con mqtt_lib.h. El tópico identifica sucursal y nodo.
"""
MQTT_REQ_ERASE_NVS = 0x01
MQTT_RESP_ERASE_NVS = 0x81
import struct

MQTT_REQ_OCCUPANCY_ADJUST = 0x02
MQTT_RESP_OCCUPANCY_ADJUST = 0x82
MQTT_NVS_WIFI = 0x01  # Reservado, sin implementación remota todavía
MQTT_NVS_TCP = 0x02   # Reservado, sin implementación remota todavía
MQTT_NVS_IDENTITY = 0x03


def reset_frame():
    return bytes((MQTT_REQ_ERASE_NVS, MQTT_NVS_IDENTITY))


def occupancy_adjust_frame(value):
    return bytes((MQTT_REQ_OCCUPANCY_ADJUST,)) + struct.pack('<i', int(value))


def parse_reset_result(payload):
    if len(payload) != 3 or payload[:2] != bytes((MQTT_RESP_ERASE_NVS, MQTT_NVS_IDENTITY)):
        raise ValueError('Respuesta de borrado inválida')
    if payload[2] not in (0, 1):
        raise ValueError('Resultado inválido')
    return payload[2] == 0


def parse_occupancy_adjust_result(payload):
    if len(payload) != 3 or payload[0] != MQTT_RESP_OCCUPANCY_ADJUST:
        raise ValueError('Respuesta de ajuste de aforo inválida')
    if payload[2] not in (0, 1):
        raise ValueError('Resultado inválido')
    return payload[2] == 0
