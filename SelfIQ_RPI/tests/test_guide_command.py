import asyncio
import json
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from urllib.error import HTTPError

from selfiq.backend_link import BackendLink
from selfiq.mqtt_bridge import MQTTBridge

NODE = '11111111-1111-4111-8111-111111111111'
BRANCH = '22222222-2222-4222-8222-222222222222'
SESSION = 'd2b6c1a0-1111-4111-8111-111111111111'
BOOT = '1f3ac9d2'
MODULE = 'shelf_monitor'
COMMAND_TOPIC = f'selfiq/{BRANCH}/{NODE}/{MODULE}/command'
ACK_TOPIC = COMMAND_TOPIC + '/ack'
STATUS_TOPIC = f'selfiq/{BRANCH}/{NODE}/{MODULE}/status'
CAPS_TOPIC = f'selfiq/{BRANCH}/{NODE}/{MODULE}/capabilities'


def mqtt_message(topic, payload, retain=False):
    if not isinstance(payload, (bytes, bytearray)):
        payload = json.dumps(payload).encode()
    return SimpleNamespace(topic=topic, retain=retain, payload=payload)


def ok_info():
    return Mock(rc=0)


def result(command_id, applied='on', reason='ok', channel=2, session=SESSION, uptime=2000):
    return {'v': 1, 't': 'guide.result', 'id': command_id, 'session': session,
            'ch': channel, 'boot': BOOT, 'applied': applied, 'reason': reason,
            'uptime_ms': uptime}


def capabilities(**overrides):
    return {'v': 1, 't': 'guide.capabilities', 'boot': BOOT, 'driver': 'direct',
            'channels': 8, 'channels_map': [0, 1, 3, 4, 5, 6, 7, 10], **overrides}


class GuideCommandTests(unittest.TestCase):
    def setUp(self):
        self.bridge = MQTTBridge(SimpleNamespace(), SimpleNamespace(branch_id=BRANCH))
        self.bridge.connected = True
        self.bridge.subscriptions_ready.set()
        self.bridge.client = Mock()
        self.sent = []
        self.command = {'node_id': NODE, 'branch_id': BRANCH, 'session_id': SESSION,
                        'channel': 2, 'ttl_ms': 60000, 'accept_window_ms': 3000}

    def status_sample(self, uptime=1000):
        self.bridge._on_message(None, None, mqtt_message(STATUS_TOPIC, {
            'v': 1, 't': 'status', 'boot': BOOT, 'uptime_ms': uptime}))

    def answer(self, expectations=None):
        """Publica la respuesta del nodo desde el propio side_effect de publish."""
        def publish(topic, payload, qos, retain):
            wire = json.loads(payload)
            self.sent.append(wire)
            self.assertEqual(topic, COMMAND_TOPIC)
            self.assertEqual(qos, 1)
            self.assertFalse(retain)
            if expectations:
                expectations(wire)
            if wire.get('t') == 'guide.status':
                self.bridge._on_message(None, None, mqtt_message(ACK_TOPIC, {
                    'v': 1, 't': 'guide.state', 'id': wire['id'], 'boot': BOOT,
                    'uptime_ms': 5000, 'on': [], 'last_ch': 2, 'last': 'ok'}))
            elif wire.get('t') in ('guide.start', 'guide.stop'):
                applied = 'off' if wire['t'] == 'guide.stop' else 'on'
                self.bridge._on_message(None, None, mqtt_message(
                    ACK_TOPIC, result(wire['id'], applied=applied, uptime=wire['accept_ms'] + 10)))
            elif wire.get('type') == 'light.test':
                self.bridge._on_message(None, None, mqtt_message(ACK_TOPIC, {
                    'v': 1, 't': 'guide.result', 'id': 'lt-firmware-01',
                    'session': '00000000-0000-4000-8000-' + 'a' * 12,
                    'ch': wire['channel'], 'boot': BOOT, 'applied': 'on',
                    'reason': 'ok', 'uptime_ms': 1000}))
            return ok_info()
        return publish

    def test_start_uses_measured_uptime_and_waits_for_real_result(self):
        self.status_sample()
        self.bridge.client.publish.side_effect = self.answer()
        outcome = self.bridge.guide_start(dict(self.command))
        self.assertEqual(outcome['delivery'], 'applied')
        self.assertEqual(outcome['light'], 'on')
        self.assertEqual(outcome['node_boot'], BOOT)
        self.assertEqual(outcome['channel'], 2)
        self.assertEqual(outcome['session_id'], SESSION)
        wire = self.sent[0]
        self.assertEqual(wire['t'], 'guide.start')
        self.assertGreater(wire['accept_ms'], 1000)
        self.assertEqual(wire['end_ms'], wire['accept_ms'] + 60000)
        self.assertNotIn('gpio', wire)
        self.assertNotIn('channel', wire)

    def test_start_without_status_queries_state_instead_of_inventing_uptime(self):
        self.bridge.client.publish.side_effect = self.answer()
        outcome = self.bridge.guide_start(dict(self.command))
        self.assertEqual(outcome['delivery'], 'applied')
        self.assertEqual([wire['t'] for wire in self.sent], ['guide.status', 'guide.start'])
        self.assertGreater(self.sent[1]['accept_ms'], 5000)

    def test_stop_omits_end_ms_and_returns_light_off(self):
        self.status_sample()
        self.bridge.client.publish.side_effect = self.answer()
        outcome = self.bridge.guide_stop(dict(self.command))
        self.assertEqual(outcome['light'], 'off')
        self.assertEqual(self.sent[0]['t'], 'guide.stop')
        self.assertNotIn('end_ms', self.sent[0])

    def test_foreign_and_wrong_session_results_are_ignored(self):
        waiter = {'id': 'gd-abcdef0123456789', 'kind': 'start', 'expect': 'guide.result',
                  'event': threading.Event(),
                  'result': None, 'branch_id': BRANCH, 'node_id': NODE,
                  'session': SESSION, 'channel': 2}
        self.bridge._guide_waiters[waiter['id']] = waiter
        self.assertFalse(self.bridge.notify_guide_result(
            result(waiter['id'], session='otra-sesion'), BRANCH, NODE, MODULE))
        self.assertFalse(self.bridge.notify_guide_result(
            result(waiter['id']), '33333333-3333-4333-8333-333333333333', NODE, MODULE))
        self.assertFalse(waiter['event'].is_set())
        self.assertTrue(self.bridge.notify_guide_result(
            result(waiter['id']), BRANCH, NODE, MODULE))
        self.assertTrue(waiter['event'].is_set())

    def test_malformed_result_does_not_wake_waiter(self):
        waiter = {'id': 'gd-abcdef0123456789', 'kind': 'start', 'expect': 'guide.result',
                  'event': threading.Event(),
                  'result': None, 'branch_id': BRANCH, 'node_id': NODE,
                  'session': SESSION, 'channel': 2}
        self.bridge._guide_waiters[waiter['id']] = waiter
        for payload in ({'v': 1, 't': 'guide.result', 'id': waiter['id'],
                         'boot': 'XYZ', 'uptime_ms': 1},
                        {'v': 1, 't': 'guide.result', 'id': waiter['id'],
                         'boot': BOOT, 'uptime_ms': -1},
                        {'v': 2, 't': 'guide.result', 'id': waiter['id'],
                         'boot': BOOT, 'uptime_ms': 1}):
            self.assertFalse(self.bridge.notify_guide_result(payload, BRANCH, NODE, MODULE))
        self.assertFalse(waiter['event'].is_set())

    def test_capabilities_validated_stored_and_limit_channel(self):
        self.bridge._on_message(None, None, mqtt_message(CAPS_TOPIC, {
            'v': 1, 't': 'guide.capabilities', 'boot': BOOT, 'driver': 'direct',
            'channels': 8, 'channels_map': [0, 1, 3, 4, 5, 6, 7, 10], 'fw': '1.0.0'}))
        self.assertEqual(self.bridge._guide_capabilities[NODE]['channels'], 8)
        self.bridge._on_message(None, None, mqtt_message(CAPS_TOPIC, {
            'v': 1, 't': 'guide.capabilities', 'boot': BOOT, 'driver': 'direct',
            'channels': 8, 'channels_map': [0, 1]}))
        self.assertEqual(self.bridge._guide_capabilities[NODE]['channels'], 8)
        bad = dict(self.command, channel=9)
        with self.assertRaisesRegex(ValueError, 'invalid_channel'):
            self.bridge.guide_start(bad)

    def test_status_online_still_queues_heartbeat(self):
        with self.bridge._guide_lock:
            self.bridge._guide_samples.clear()
        with unittest.mock.patch.object(self.bridge, '_queue_heartbeat') as heartbeat:
            self.bridge._on_message(None, None, mqtt_message(STATUS_TOPIC, b'online'))
            heartbeat.assert_called_once_with(NODE)

    def test_current_firmware_capabilities_on_command_ack_are_not_roi(self):
        with patch.object(self.bridge, 'notify_roi_ack') as roi, \
                patch.object(self.bridge, 'notify_cal_result') as calibration, \
                patch.object(self.bridge, '_queue_guide_capabilities') as queue:
            self.bridge._on_message(None, None, mqtt_message(ACK_TOPIC, capabilities()))
            self.assertEqual(self.bridge._guide_capabilities[NODE]['channels'], 8)
            queue.assert_called_once_with(NODE, self.bridge._guide_capabilities[NODE])
            roi.assert_not_called()
            calibration.assert_not_called()

    def test_capabilities_reject_retained_wrong_branch_camera_and_invalid_payload(self):
        for topic in (CAPS_TOPIC, ACK_TOPIC):
            for message in (
                mqtt_message(topic, capabilities(), retain=True),
                mqtt_message(topic.replace(BRANCH, SESSION), capabilities()),
                mqtt_message(topic.replace(MODULE, 'shelf_cam'), capabilities()),
                mqtt_message(topic, capabilities(channels_map=[1, 1])),
                mqtt_message(topic, []),
            ):
                with self.subTest(topic=message.topic, retain=message.retain):
                    self.bridge._on_message(None, None, message)
                    self.assertFalse(self.bridge._guide_capabilities)

    def test_light_test_waits_for_node_result_not_puback(self):
        self.bridge.client.publish.side_effect = self.answer()
        outcome = self.bridge.test_light({'node_id': NODE, 'branch_id': BRANCH,
                                          'channel': 5, 'duration_ms': 5000})
        self.assertEqual(outcome['status'], 'applied')
        self.assertEqual(outcome['light'], 'on')
        self.assertEqual(outcome['channel'], 5)
        self.assertEqual(self.sent[0]['type'], 'light.test')
        self.assertFalse(self.bridge._guide_waiters)

    def test_disconnect_frees_waiters_without_inventing_success(self):
        waiter = {'id': 'gd-abcdef0123456789', 'kind': 'start', 'expect': 'guide.result',
                  'event': threading.Event(),
                  'result': None, 'unknown': False, 'branch_id': BRANCH,
                  'node_id': NODE, 'session': SESSION, 'channel': 2}
        self.bridge._guide_waiters[waiter['id']] = waiter
        self.bridge._on_disconnect(None, None, None, 0, None)
        self.assertFalse(self.bridge._guide_waiters)
        self.assertTrue(waiter['unknown'])
        self.assertTrue(waiter['event'].is_set())


class WhitelistTests(unittest.IsolatedAsyncioTestCase):
    async def test_guide_commands_are_allowed(self):
        link = BackendLink(SimpleNamespace(), lambda command, message: asyncio.sleep(
            0, result={'delivery': 'applied'}))
        for command in ('guide.start', 'guide.stop', 'guide.status'):
            with self.subTest(command=command):
                reply = await link.process(json.dumps(
                    {'type': 'command', 'id': 'cmd-' + command, 'command': command}))
                self.assertTrue(reply['ok'], reply)
        rejected = await link.process(json.dumps(
            {'type': 'command', 'id': 'cmd-other', 'command': 'guide.restart'}))
        self.assertFalse(rejected['ok'])


class CapabilitiesUploadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        backend = SimpleNamespace(connected=True, branch_id=BRANCH,
                                  ensure_identity=Mock(return_value=SimpleNamespace(
                                      key=Mock(sign=Mock(return_value=b'signed')))))
        config = SimpleNamespace(gateway_id='rpi-test', backend_ws_url='https://backend.example')
        self.bridge = MQTTBridge(config, backend)
        self.bridge.loop = asyncio.get_running_loop()
        self.bridge._post_signed = Mock(return_value={'ok': True, 'node_id': NODE, 'channels': 8})

    async def receive(self):
        self.bridge._on_message(None, None, mqtt_message(ACK_TOPIC, capabilities()))
        await asyncio.sleep(0)  # Ejecutar el callback encolado desde MQTT.

    async def test_announcement_before_backend_connect_is_retried_without_new_announcement(self):
        self.bridge.backend.connected = False
        await self.receive()
        await self.bridge._flush_guide_capabilities()
        self.bridge._post_signed.assert_not_called()
        self.assertIn(NODE, self.bridge._guide_capabilities_pending)
        self.bridge.backend.connected = True
        await self.bridge._flush_guide_capabilities()
        url, body = self.bridge._post_signed.call_args.args
        self.assertIn(f'/gateways/rpi-test/nodes/{NODE}/guide-capabilities', url)
        self.assertEqual(body['channels_map'], [0, 1, 3, 4, 5, 6, 7, 10])
        self.assertEqual(body['boot'], BOOT)
        self.assertIn('signature', body)
        self.assertNotIn(NODE, self.bridge._guide_capabilities_pending)

    async def test_http_failure_preserves_capabilities_until_confirmed(self):
        await self.receive()
        self.bridge._post_signed.side_effect = [
            HTTPError('https://backend.example', 503, 'unavailable', {}, None),
            {'ok': True, 'node_id': NODE, 'channels': 8},
        ]
        await self.bridge._flush_guide_capabilities()
        self.assertIn(NODE, self.bridge._guide_capabilities_pending)
        self.assertNotIn(NODE, self.bridge._guide_capabilities_confirmed)
        await self.bridge._flush_guide_capabilities()
        self.assertFalse(self.bridge._guide_capabilities_pending)
        self.assertIn(NODE, self.bridge._guide_capabilities_confirmed)

    async def test_unconfirmed_or_foreign_response_does_not_clear_pending(self):
        await self.receive()
        for response in ({'ok': False}, {'ok': True, 'node_id': SESSION, 'channels': 8},
                         {'ok': True, 'node_id': NODE, 'channels': 4}):
            self.bridge._post_signed.return_value = response
            await self.bridge._flush_guide_capabilities()
            self.assertIn(NODE, self.bridge._guide_capabilities_pending)

    async def test_newer_boot_received_during_http_is_not_lost(self):
        await self.receive()
        newer = self.bridge._validate_guide_capabilities(capabilities(boot='1234abcd'), BRANCH)
        enqueued = asyncio.Event()
        def post(*_args):
            self.bridge._queue_guide_capabilities(NODE, newer)
            self.bridge.loop.call_soon_threadsafe(enqueued.set)
            return {'ok': True, 'node_id': NODE, 'channels': 8}
        self.bridge._post_signed.side_effect = post
        await self.bridge._flush_guide_capabilities()
        await enqueued.wait()
        self.assertIs(self.bridge._guide_capabilities_pending[NODE], newer)

    async def test_heartbeat_retries_and_throttles_only_confirmed_capabilities(self):
        await self.receive()
        await self.bridge._flush_guide_capabilities()
        with patch.object(self.bridge, '_send_heartbeat', new_callable=AsyncMock):
            self.bridge._queue_heartbeat(NODE)
            await asyncio.sleep(0)
            self.assertFalse(self.bridge._guide_capabilities_pending)
            caps = self.bridge._guide_capabilities[NODE]
            self.bridge._guide_capabilities_confirmed[NODE] = (caps, time.monotonic() - 61)
            self.bridge._queue_heartbeat(NODE)
            await asyncio.sleep(0)
            self.assertIn(NODE, self.bridge._guide_capabilities_pending)

    async def test_worker_is_stopped_with_bridge(self):
        flushed = asyncio.Event()
        async def flush():
            flushed.set()
        self.bridge._flush_guide_capabilities = flush
        worker = asyncio.create_task(self.bridge._guide_capability_uploads())
        self.bridge._guide_capabilities_worker = worker
        await flushed.wait()
        await self.bridge.stop()
        self.assertTrue(worker.cancelled())
        self.assertIsNone(self.bridge._guide_capabilities_worker)


if __name__ == '__main__':
    unittest.main()
