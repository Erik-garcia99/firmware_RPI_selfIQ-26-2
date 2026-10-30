import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from selfiq.mqtt_bridge import MQTTBridge
from selfiq.backend_link import BackendLink
from selfiq.node_control import reset_frame, parse_reset_result

NODE = '11111111-1111-4111-8111-111111111111'
BRANCH = '22222222-2222-4222-8222-222222222222'


class ResetTests(unittest.TestCase):
    def setUp(self):
        self.bridge = MQTTBridge(SimpleNamespace(), None)
        self.bridge.connected = True
        self.bridge.subscriptions_ready.set()
        self.bridge.client = Mock()

    def reply(self, topic, payload, qos, retain):
        self.assertEqual(qos, 1)
        self.assertFalse(retain)
        self.assertEqual(payload, bytes.fromhex('0103'))
        response = bytes.fromhex('810300')
        self.bridge._on_message(None, None, SimpleNamespace(
            topic=topic.removesuffix('command') + 'result', retain=False,
            payload=response))
        return SimpleNamespace(rc=0)

    def test_complete_backend_command_waits_for_node_confirmation(self):
        self.bridge.client.publish.side_effect = self.reply

        async def execute(command, message):
            self.assertEqual(command, 'node.reset')
            return await asyncio.to_thread(self.bridge.reset_node, message)

        link = BackendLink(SimpleNamespace(), execute)
        result = asyncio.run(link.process(json.dumps({'type': 'command', 'id': 'reset-test',
            'command': 'node.reset', 'node_id': NODE, 'branch_id': BRANCH})))
        self.assertTrue(result['ok'])
        self.assertEqual(result['data'], {'identity_reset': True, 'node_id': NODE})
        self.assertFalse(self.bridge.reset_waiters)

    def test_broker_publish_alone_does_not_confirm_reset(self):
        self.bridge.client.publish.return_value = SimpleNamespace(rc=0)
        with patch('selfiq.mqtt_bridge.threading.Event') as event:
            event.return_value.wait.return_value = False
            with self.assertRaisesRegex(ValueError, 'no confirmó'):
                self.bridge.reset_node({'node_id': NODE, 'branch_id': BRANCH})
        self.assertFalse(self.bridge.reset_waiters)

    def test_retained_wrong_topic_or_wrong_target_cannot_confirm(self):
        waiter = {'event': Mock(), 'topic': f'selfiq/{BRANCH}/{NODE}/control/result'}
        self.bridge.reset_waiters[waiter['topic']] = waiter
        for topic, retained, payload in ((waiter['topic'], True, b'\x81\x03\x00'),
                ('selfiq/other/node/control/result', False, b'\x81\x03\x00'),
                (waiter['topic'], False, b'\x81\x01\x00')):
            self.bridge._on_message(None, None, SimpleNamespace(topic=topic, retain=retained,
                payload=payload))
        waiter['event'].set.assert_not_called()

    def test_wire_vectors_and_malformed_frames(self):
        self.assertEqual(reset_frame(), bytes.fromhex('0103'))
        frame = bytes.fromhex('810300')
        self.assertTrue(parse_reset_result(frame))
        self.assertFalse(parse_reset_result(frame[:-1] + b'\x01'))
        for invalid in (frame[:-1], frame + b'\x00', b'\x00' + frame[1:],
                        frame[:2] + b'\x15' + frame[3:],
                        b'\x81\x02\x00',
                        frame[:-1] + b'\x02'):
            with self.assertRaises(ValueError):
                parse_reset_result(invalid)

    def test_only_one_pending_erase_per_node(self):
        topic = f'selfiq/{BRANCH}/{NODE}/control/result'
        self.bridge.reset_waiters[topic] = object()
        with self.assertRaisesRegex(ValueError, 'pendiente'):
            self.bridge.reset_node({'node_id': NODE, 'branch_id': BRANCH})
        self.bridge.client.publish.assert_not_called()
