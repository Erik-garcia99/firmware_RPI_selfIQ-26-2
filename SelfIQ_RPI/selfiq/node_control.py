"""Payload MQTT: comando + destino NVS; respuesta añade resultado.

Valores compartidos con mqtt_lib.h. El tópico identifica sucursal y nodo.
"""
MQTT_REQ_ERASE_NVS = 0x01
MQTT_RESP_ERASE_NVS = 0x81
MQTT_NVS_WIFI = 0x01  # Reservado, sin implementación remota todavía
MQTT_NVS_TCP = 0x02   # Reservado, sin implementación remota todavía
MQTT_NVS_IDENTITY = 0x03


def reset_frame():
    return bytes((MQTT_REQ_ERASE_NVS, MQTT_NVS_IDENTITY))


def parse_reset_result(payload):
    if len(payload) != 3 or payload[:2] != bytes((MQTT_RESP_ERASE_NVS, MQTT_NVS_IDENTITY)):
        raise ValueError('Respuesta de borrado inválida')
    if payload[2] not in (0, 1):
        raise ValueError('Resultado inválido')
    return payload[2] == 0
