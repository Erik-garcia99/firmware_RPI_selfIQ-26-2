import unittest
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock, patch

from selfiq.influx_sync import send_batch
from selfiq.mqtt_bridge import MQTTBridge
from selfiq.app import create_app
from fastapi.testclient import TestClient

BRANCH = '11111111-1111-4111-8111-111111111111'
NODE = '22222222-2222-4222-8222-222222222222'


def event(name='one', node=NODE, received=200):
    return {'event_id': name, 'topic': f'selfiq/{BRANCH}/{node}/shelf_cam/telemetry',
            'received_at': received, 'payload': {'event_type': 'cam_fps', 'data': {'value': 4}}}


class RegistrationGateTests(unittest.TestCase):
    def setUp(self):
        self.backend = SimpleNamespace(connected=True, registration_status='registered',
                                       branch_id=BRANCH, ensure_identity=Mock())
        self.backend.ensure_identity.return_value.key.sign.return_value = b'signature'
        self.bridge = MQTTBridge(SimpleNamespace(gateway_id='rpi-test',
                                 backend_ws_url='wss://example.com/ws/gateways'), self.backend)
        self.bridge._post_signed = Mock(return_value={
            'id': NODE, 'branch_id': BRANCH, 'authorized': True, 'registered_at': 100})

    @patch('selfiq.influx_sync.enviar_influx')
    def test_restart_needs_backend_proof_even_without_local_registration(self, write):
        self.assertEqual(send_batch(None, [event()], self.bridge.authorize_node), ['one'])
        write.assert_called_once()
        self.assertIn('/authorize', self.bridge._post_signed.call_args.args[0])

    @patch('selfiq.influx_sync.enviar_influx')
    def test_deleted_pending_foreign_and_pre_registration_events_never_leave(self, write):
        for response in ({'authorized': False}, {'authorized': True, 'registered_at': 300}):
            self.bridge._post_signed.return_value.update(response)
            self.assertEqual(send_batch(None, [event()], self.bridge.authorize_node), ['one'])
        self.assertIsNone(self.bridge.authorize_node(NODE, NODE))
        write.assert_not_called()

    @patch('selfiq.influx_sync.enviar_influx')
    def test_offline_or_failed_authorization_preserves_batch(self, write):
        self.backend.connected = False
        with self.assertRaises(RuntimeError):
            send_batch(None, [event()], self.bridge.authorize_node)
        self.backend.connected = True
        self.bridge._post_signed.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            send_batch(None, [event()], self.bridge.authorize_node)
        write.assert_not_called()

    @patch('selfiq.influx_sync.enviar_influx')
    def test_revocation_rechecked_next_batch(self, write):
        send_batch(None, [event()], self.bridge.authorize_node)
        self.bridge._post_signed.return_value['authorized'] = False
        send_batch(None, [event('two')], self.bridge.authorize_node)
        self.assertEqual(write.call_count, 1)
        self.assertEqual(self.bridge._post_signed.call_count, 2)

    @patch('selfiq.influx_sync.enviar_influx')
    def test_mixed_batch_sends_only_authorized_node(self, write):
        other = '33333333-3333-4333-8333-333333333333'
        authorize = Mock(side_effect=lambda branch, node: 100 if node == NODE else None)
        self.assertEqual(send_batch(None, [event(), event('two', other)], authorize), ['one', 'two'])
        self.assertIn(NODE, write.call_args.args[1])
        self.assertNotIn(other, write.call_args.args[1])

    @patch('selfiq.influx_sync.enviar_influx')
    def test_malformed_backend_response_does_not_authorize(self, write):
        self.bridge._post_signed.return_value = {'id': NODE, 'authorized': True}
        self.assertEqual(send_batch(None, [event()], self.bridge.authorize_node), [])
        write.assert_not_called()


class SnapshotGateTests(unittest.TestCase):
    def setUp(self):
        with patch('selfiq.app.Controller'):
            self.app = create_app(SimpleNamespace(ap_ip='192.168.50.1'), network=Mock())
        self.client = TestClient(self.app, client=('127.0.0.1', 1234))
        self.addCleanup(self.client.close)
        self.bridge = self.app.state.mqtt_bridge
        self.bridge.authorize_node = Mock(return_value=None)
        self.bridge.notify_snapshot = Mock()
        self.headers = {'X-Node-Id': NODE, 'X-Branch-Id': BRANCH, 'X-Req-Id': 'test'}

    def test_unknown_node_cannot_upload_or_ack_snapshot(self):
        response = self.client.post('/camera/snapshot', headers=self.headers, content=b'jpeg')
        self.assertEqual(response.status_code, 403)
        self.bridge.notify_snapshot.assert_not_called()
        self.assertEqual(self.client.get('/camera/snapshot', params={'node_id': NODE}).status_code, 404)

    def test_backend_unavailable_blocks_snapshot(self):
        self.bridge.authorize_node.side_effect = TimeoutError()
        self.assertEqual(self.client.post('/camera/snapshot', headers=self.headers,
                                         content=b'jpeg').status_code, 503)
        self.bridge.notify_snapshot.assert_not_called()

    def test_revoked_node_cannot_read_previously_cached_photo(self):
        self.bridge.authorize_node.return_value = 100
        self.assertEqual(self.client.post('/camera/snapshot', headers=self.headers,
                                         content=b'jpeg').status_code, 200)
        self.bridge.authorize_node.return_value = None
        self.assertEqual(self.client.get('/camera/snapshot', params={'node_id': NODE}).status_code, 403)

    def test_unregistered_node_cannot_execute_operational_commands(self):
        for command in ('snapshot.request', 'roi.set', 'light.test'):
            with self.subTest(command=command), self.assertRaises(ValueError):
                asyncio.run(self.app.state.execute_command(command,
                    {'node_id': NODE, 'branch_id': BRANCH}))
