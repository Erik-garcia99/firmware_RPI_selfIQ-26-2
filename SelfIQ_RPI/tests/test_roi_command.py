import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from selfiq.mqtt_bridge import MQTTBridge
from selfiq.backend_link import BackendLink

NODE = '11111111-1111-4111-8111-111111111111'
BRANCH = '22222222-2222-4222-8222-222222222222'
COMMAND_TOPIC = f'selfiq/{BRANCH}/{NODE}/shelf_cam/command'
ACK_TOPIC = COMMAND_TOPIC + '/ack'


def ack(ok=True, req_id='req-1', version=4, count=2, error=None):
    body = {'type': 'roi_ack', 'req_id': req_id, 'ok': ok}
    if version is not None:
        body['version'] = version
    if count is not None:
        body['count'] = count
    if error:
        body['error'] = error
    return json.dumps(body).encode()


class RoiCommandTests(unittest.TestCase):
    def setUp(self):
        self.bridge = MQTTBridge(SimpleNamespace(), SimpleNamespace(branch_id=BRANCH))
        self.bridge.connected = True
        self.bridge.subscriptions_ready.set()
        self.bridge.client = Mock()
        self.command = {
            'node_id': NODE,
            'branch_id': BRANCH,
            'module': 'shelf_cam',
            'req_id': 'req-1',
            'version': 4,
            'rois': [{'id': 'roi-1', 'x': .1, 'y': .2, 'w': .3, 'h': .25}],
        }

    def ack_on_publish(self, body):
        def reply(topic, payload, qos, retain):
            self.assertEqual(topic, COMMAND_TOPIC)
            self.assertEqual(qos, 1)
            self.assertFalse(retain)
            sent = json.loads(payload)
            self.assertEqual(sent['type'], 'roi.set')
            self.assertEqual(sent['req_id'], 'req-1')
            self.assertEqual(sent['version'], 4)
            self.bridge._on_message(None, None, SimpleNamespace(
                topic=ACK_TOPIC, retain=False, payload=body))
            return Mock(rc=0)
        return reply

    def test_publishes_and_waits_for_matching_ack(self):
        self.bridge.client.publish.side_effect = self.ack_on_publish(ack())
        result = self.bridge.set_rois(dict(self.command))
        self.assertTrue(result['rois'])
        self.assertTrue(result['ok'])
        self.assertEqual(result['version'], 4)
        self.assertEqual(result['count'], 2)
        self.assertEqual(result['req_id'], 'req-1')
        self.assertFalse(self.bridge._roi_waiters)

    def test_ack_with_other_req_id_does_not_wake_waiter(self):
        waiter = {'event': Mock(), 'req_id': 'otro', 'result': None}
        self.bridge._roi_waiters['otro'] = waiter
        self.assertFalse(self.bridge.notify_roi_ack(ack(req_id='distinto')))
        waiter['event'].set.assert_not_called()

    def test_ack_ok_false_returns_node_error(self):
        self.bridge.client.publish.side_effect = self.ack_on_publish(
            ack(ok=False, error='invalid_bbox', version=None, count=None))
        with self.assertRaisesRegex(ValueError, 'invalid_bbox'):
            self.bridge.set_rois(dict(self.command))
        self.assertFalse(self.bridge._roi_waiters)

    def test_timeout_reports_clear_error(self):
        self.bridge.client.publish.return_value = Mock(rc=0)
        with patch('selfiq.mqtt_bridge.threading.Event') as event:
            event.return_value.wait.return_value = False
            with self.assertRaisesRegex(ValueError, 'timeout'):
                self.bridge.set_rois(dict(self.command))
        self.assertFalse(self.bridge._roi_waiters)

    def test_rejects_invalid_module_and_payload(self):
        for change in ({'module': 'guide'}, {'module': 'bad module'}, {'req_id': ''},
                       {'version': True}, {'rois': 'x'}, {'rois': []},
                       {'rois': [{'id': 'r', 'x': 'a', 'y': 0, 'w': 0, 'h': 0}]}):
            with self.subTest(change=change):
                with self.assertRaises(ValueError):
                    self.bridge.set_rois({**self.command, **change})
        self.bridge.client.publish.assert_not_called()

    def test_ack_does_not_enter_outbox(self):
        self.bridge.outbox = Mock()
        self.bridge._roi_waiters['req-1'] = {'event': Mock(), 'req_id': 'req-1', 'result': None}
        self.bridge._on_message(None, None, SimpleNamespace(
            topic=ACK_TOPIC, retain=False, payload=ack()))
        self.bridge.outbox.put.assert_not_called()

    def test_retained_or_oversized_ack_is_ignored(self):
        waiter = {'event': Mock(), 'req_id': 'req-1', 'result': None}
        self.bridge._roi_waiters['req-1'] = waiter
        for retained, body in ((True, ack()), (False, b'x' * 1025)):
            self.bridge._on_message(None, None, SimpleNamespace(
                topic=ACK_TOPIC, retain=retained, payload=body))
        waiter['event'].set.assert_not_called()

    def test_foreign_branch_ack_is_ignored(self):
        waiter = {'event': Mock(), 'req_id': 'req-1', 'result': None}
        self.bridge._roi_waiters['req-1'] = waiter
        topic = f'selfiq/{NODE}/{NODE}/shelf_cam/command/ack'
        self.bridge._on_message(None, None, SimpleNamespace(
            topic=topic, retain=False, payload=ack()))
        waiter['event'].set.assert_not_called()

    def test_subscribes_to_command_ack(self):
        client = Mock()
        client.subscribe.return_value = (0, 7)
        self.bridge._on_connect(client, None, None, 0, None)
        topics = client.subscribe.call_args.args[0]
        self.assertIn(('selfiq/+/+/+/command/ack', 1), topics)

    def test_backend_link_allows_roi_set(self):
        async def execute(command, message):
            self.assertEqual(command, 'roi.set')
            return {'rois': True, 'version': 4, 'count': 1, 'req_id': 'req-1'}

        link = BackendLink(SimpleNamespace(), execute)
        result = asyncio.run(link.process(json.dumps({
            'type': 'command', 'id': 'roi-1', 'command': 'roi.set',
            'node_id': NODE, 'branch_id': BRANCH})))
        self.assertTrue(result['ok'])
        self.assertTrue(result['data']['rois'])


if __name__ == '__main__':
    unittest.main()
