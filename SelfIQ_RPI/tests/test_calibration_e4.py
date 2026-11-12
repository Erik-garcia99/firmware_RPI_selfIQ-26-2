import hashlib
import sys
import types
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

# El entorno de revisión no trae paho-mqtt; el gateway real sí lo instala.
# Stub mínimo para probar la lógica de E4 sin abrir sockets MQTT.
paho = types.ModuleType('paho')
paho_mqtt = types.ModuleType('paho.mqtt')
paho_client = types.ModuleType('paho.mqtt.client')
paho_client.MQTT_ERR_SUCCESS = 0
paho_client.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
paho_client.Client = object
sys.modules.setdefault('paho', paho)
sys.modules.setdefault('paho.mqtt', paho_mqtt)
sys.modules.setdefault('paho.mqtt.client', paho_client)

from selfiq.calibration import (
    CalibrationCommandError,
    CalibrationStore,
    command_fingerprint,
    parse_multipart_reference,
    validate_ack,
    validate_command,
)
from selfiq.mqtt_bridge import MQTTBridge


def uid():
    return str(uuid4())


def command(kind='empty'):
    now = '2026-11-09T20:00:00.000Z'
    return {
        'schema_version': 1,
        'type': 'calibrate',
        'id': uid(),
        'req_id': uid(),
        'branch_id': uid(),
        'node_id': uid(),
        'calibration_id': uid(),
        'calibration_version': 4,
        'roi_id': uid(),
        'config_version': 9,
        'context_hash': 'a' * 64,
        'capture_id': uid(),
        'kind': kind,
        'format_version': 1,
        'issued_at': now,
        'expires_at': '2026-11-09T20:00:10.000Z',
    }


def ack_for(cmd):
    return {
        'schema_version': 1,
        'type': 'cal_result',
        'id': cmd['id'],
        'req_id': cmd['req_id'],
        'ok': True,
        'action': 'capture',
        'status': 'stored',
        'calibration_id': cmd['calibration_id'],
        'calibration_version': cmd['calibration_version'],
        'roi_id': cmd['roi_id'],
        'config_version': cmd['config_version'],
        'context_hash': cmd['context_hash'],
        'capture_id': cmd['capture_id'],
        'kind': cmd['kind'],
        'format_version': cmd['format_version'],
        'descriptor_crc': 'deadbeef',
        'image_sha256': 'b' * 64,
        'image_bytes': 1234,
        'width': 640,
        'height': 480,
        'quality': {'mean': 110.0, 'contrast': 20.0, 'clipped_pct': 0.5},
        'clock_quality': 'unknown',
        'uptime_ms': 1234,
        'persisted': True,
    }


class FakePublishInfo:
    rc = 0
    def wait_for_publish(self, timeout=None):
        return True


class FakeClient:
    def __init__(self):
        self.published = []
        self.on_publish = None

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return FakePublishInfo()


class CalibrationContractTests(unittest.TestCase):
    def test_status_accepts_partial_draft_but_not_missing_crc(self):
        cmd = validate_command({**command(), 'type': 'calibration.status'})
        ack = {**ack_for(cmd), 'action': 'status', 'status': 'reported',
               'active': None, 'draft': {
                   'calibration_id': cmd['calibration_id'],
                   'calibration_version': cmd['calibration_version'],
                   'empty_crc': 'deadbeef', 'full_crc': None}}
        validate_ack(cmd, ack)
        del ack['draft']['full_crc']
        with self.assertRaises(CalibrationCommandError):
            validate_ack(cmd, ack)

    def test_cancel_requires_version_in_ack(self):
        cmd = validate_command({**command(), 'type': 'calibration.cancel'})
        ack = {**ack_for(cmd), 'action': 'cancel', 'status': 'cancelled'}
        validate_ack(cmd, ack)
        del ack['calibration_version']
        with self.assertRaises(CalibrationCommandError):
            validate_ack(cmd, ack)

    def test_validate_command_accepts_contract_without_action_field(self):
        raw = command()
        normalized = validate_command(raw)
        self.assertEqual(normalized['_action'], 'capture')
        self.assertEqual(normalized['id'], raw['id'])

    def test_semantic_fingerprint_ignores_transport_attempt(self):
        first = validate_command(command())
        second = dict(first)
        second['req_id'] = uid()
        second['issued_at'] = '2026-11-09T20:00:01.000Z'
        second['expires_at'] = '2026-11-09T20:00:11.000Z'
        self.assertEqual(command_fingerprint(first), command_fingerprint(second))
        second['capture_id'] = uid()
        self.assertNotEqual(command_fingerprint(first), command_fingerprint(second))

    def test_ack_rejects_other_context(self):
        cmd = validate_command(command())
        ack = ack_for(cmd)
        ack['context_hash'] = 'c' * 64
        with self.assertRaises(CalibrationCommandError):
            validate_ack(cmd, ack)

    def test_store_survives_reopen_and_replays_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'cal.sqlite3'
            cmd = validate_command(command())
            result = ack_for(cmd)
            store = CalibrationStore(path)
            store.begin_operation(cmd, command_fingerprint(cmd))
            store.complete_operation(cmd['id'], result)
            store.close()
            reopened = CalibrationStore(path)
            row = reopened.operation(cmd['id'])
            self.assertEqual(row['status'], 'acknowledged')
            self.assertEqual(json.loads(row['result'])['capture_id'], cmd['capture_id'])
            reopened.close()

    def test_multipart_reference_extracts_meta_and_image(self):
        image = b'\xff\xd8\xff' + b'x' * 16
        meta = {
            'calibration_id': uid(), 'calibration_version': 1,
            'roi_id': uid(), 'capture_id': uid(), 'kind': 'empty',
            'context_hash': 'a' * 64, 'descriptor_crc': 'deadbeef',
            'format_version': 1, 'captured_at': '2026-11-09T20:00:00.000Z',
            'clock_quality': 'unknown', 'uptime_ms': 5,
            'image_sha256': hashlib.sha256(image).hexdigest(),
            'image_bytes': len(image), 'width': 640, 'height': 480,
        }
        boundary = 'selfiq-test-boundary'
        body = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="meta"\r\n\r\n'
            + json.dumps(meta)
            + f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="ref.jpg"\r\n'
              'Content-Type: image/jpeg\r\n\r\n'
        ).encode() + image + f'\r\n--{boundary}--\r\n'.encode()
        parsed_meta, parsed_image = parse_multipart_reference(
            f'multipart/form-data; boundary={boundary}', body)
        self.assertEqual(parsed_meta['capture_id'], meta['capture_id'])
        self.assertEqual(parsed_image, image)


class GatewayDispatchTests(unittest.TestCase):
    def _bridge(self, tmp):
        config = SimpleNamespace(offline_db=str(Path(tmp) / 'outbox.sqlite3'),
                                 gateway_id='gw-test', backend_ws_url='ws://localhost/ws/gateways')
        backend = SimpleNamespace(branch_id=None, connected=True,
                                  registration_status='registered')
        bridge = MQTTBridge(config, backend)
        bridge.client = FakeClient()
        bridge.connected = True
        bridge.subscriptions_ready.set()
        return bridge

    def test_cal_result_is_correlated_with_topic_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            bridge = self._bridge(tmp)
            cmd = validate_command(command())
            waiter = {'event': threading.Event(), 'command': cmd,
                      'ack': None, 'error': None}
            bridge._cal_waiters[cmd['req_id']] = waiter
            self.assertFalse(bridge.notify_cal_result(
                ack_for(cmd), cmd['branch_id'], uid(), 'shelf_cam'))
            self.assertFalse(waiter['event'].is_set())
            self.assertTrue(bridge.notify_cal_result(
                ack_for(cmd), cmd['branch_id'], cmd['node_id'], 'shelf_cam'))
            self.assertTrue(waiter['event'].is_set())

    def test_run_calibration_publishes_contract_without_private_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            bridge = self._bridge(tmp)
            raw = command()

            def answer():
                deadline = time.time() + 2
                while time.time() < deadline:
                    if bridge.client.published:
                        sent = json.loads(bridge.client.published[0][1])
                        bridge.notify_cal_result(
                            ack_for(sent), sent['branch_id'], sent['node_id'], 'shelf_cam')
                        return
                    time.sleep(0.01)

            thread = threading.Thread(target=answer)
            thread.start()
            result = bridge.run_calibration(raw)
            thread.join(2)
            sent = json.loads(bridge.client.published[0][1])
            self.assertNotIn('_action', sent)
            self.assertEqual(result['capture_id'], raw['capture_id'])
            self.assertEqual(bridge.client.published[0][0],
                             f"selfiq/{raw['branch_id']}/{raw['node_id']}/shelf_cam/command")
            bridge.cal_store.close()

    def test_completed_operation_reuses_result_for_new_req_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            bridge = self._bridge(tmp)
            first = command()

            def answer():
                deadline = time.time() + 2
                while time.time() < deadline:
                    if bridge.client.published:
                        sent = json.loads(bridge.client.published[0][1])
                        bridge.notify_cal_result(
                            ack_for(sent), sent['branch_id'], sent['node_id'], 'shelf_cam')
                        return
                    time.sleep(0.01)

            t = threading.Thread(target=answer)
            t.start()
            original = bridge.run_calibration(first)
            t.join(2)
            retry = dict(first)
            retry['req_id'] = uid()
            retry['issued_at'] = '2026-11-09T20:00:01.000Z'
            retry['expires_at'] = '2026-11-09T20:00:11.000Z'
            replay = bridge.run_calibration(retry)
            self.assertEqual(replay['id'], original['id'])
            self.assertEqual(replay['req_id'], retry['req_id'])
            self.assertEqual(len(bridge.client.published), 1)
            bridge.cal_store.close()


if __name__ == '__main__':
    unittest.main()
