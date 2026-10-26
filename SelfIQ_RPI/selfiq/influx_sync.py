
import asyncio
import logging
import math
import re
from decimal import Decimal
from urllib.error import HTTPError

from .influx_writer import enviar_influx


log = logging.getLogger(__name__)

BATCH_SIZE = 100
SYNC_INTERVAL_SECONDS = 5
RETRY_INTERVAL_SECONDS = 60
NANOSECONDS_PER_SECOND = 1_000_000_000

NAME_PATTERN = re.compile(r"[a-z0-9_]{1,32}")
EVENT_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,96}")


def escape_tag(value):
    """Escapa un valor usado como tag en line protocol."""
    value = str(value)

    if "\n" in value or "\r" in value:
        raise ValueError("Tag con salto de línea")

    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def event_to_line(event):
    parts = event["topic"].split("/")

    if (
        len(parts) != 5
        or parts[0] != "selfiq"
        or parts[4] != "telemetry"
    ):
        raise ValueError("Tópico de telemetría inválido")

    _, branch_id, esp_id, module, _ = parts

    payload = event["payload"]
    event_type = payload.get("event_type")
    event_id = event["event_id"]
    data = payload.get("data")

    if (
        not isinstance(event_type, str)
        or not NAME_PATTERN.fullmatch(event_type)
        or not EVENT_ID_PATTERN.fullmatch(event_id)
    ):
        raise ValueError("Tipo o identificador de evento inválido")

    if not isinstance(data, dict):
        raise ValueError("data debe ser un objeto")

    value = data.get("value")

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("data.value debe ser numérico")

    value = float(value)

    if not math.isfinite(value):
        raise ValueError("data.value no es finito")

    # Usa la hora de recepción guardada en SQLite.
    # No cambia al reintentar el envío.
    timestamp_ns = int(
        Decimal(str(event["received_at"])) * NANOSECONDS_PER_SECOND
    )

    tags = ",".join((
        f"branch_id={escape_tag(branch_id)}",
        f"esp_id={escape_tag(esp_id)}",
        f"module={escape_tag(module)}",
        f"event_id={escape_tag(event_id)}",
    ))

    return f"{event_type},{tags} value={value!r} {timestamp_ns}"


def send_batch(config, batch):
    lines = []
    accepted_candidates = []

    for event in batch:
        try:
            lines.append(event_to_line(event))
            accepted_candidates.append(event["event_id"])
        except (ValueError, TypeError, KeyError, OverflowError):
            # No eliminar un evento que no pudimos convertir.
            log.error(
                "[INFLUX] Evento no compatible; permanece en SQLite: %s",
                event.get("event_id", "sin_id"),
            )

    if not lines:
        return []

    # Si hay un error, la excepción llega a replay_once().
    # En ese caso no se confirma ningún ID de este envío.
    enviar_influx(config, "\n".join(lines))

    return accepted_candidates


async def run_influx_sync(config, outbox, stop_event):
    log.info("[INFLUX] Sincronización iniciada")

    while not stop_event.is_set():
        delay = SYNC_INTERVAL_SECONDS

        try:
            sent = await asyncio.to_thread(
                outbox.replay_once,
                lambda batch: send_batch(config, batch),
                BATCH_SIZE,
            )

            if sent:
                log.info(
                    "[INFLUX] API aceptó %d eventos; retirados de SQLite",
                    sent,
                )

        except HTTPError as exc:
            delay = RETRY_INTERVAL_SECONDS
            log.warning(
                "[INFLUX] HTTP %s; pendientes conservados",
                exc.code,
            )

        except Exception as exc:
            delay = RETRY_INTERVAL_SECONDS
            # No imprimir credenciales ni el contenido de la solicitud.
            log.warning(
                "[INFLUX] Falló el envío (%s); pendientes conservados",
                type(exc).__name__,
            )

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    log.info("[INFLUX] Sincronización detenida")

