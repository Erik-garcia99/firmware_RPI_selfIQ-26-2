import json
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from selfiq.outbox import Outbox
from selfiq.influx_sync import event_to_line, send_batch

ROI = 'a5760093-b323-48d6-a1bb-d1574ab07857'
TOPIC = ('selfiq/d07018c7-99c1-490f-95b9-dd54589cf60b/'
         '5c485efa-cc20-4f9a-91eb-ceeba0f99289/shelf_cam/telemetry')


def payload(identifier='one', **changes):
    data = dict(roi_id=ROI, value=63, config_version=28,
                calibration_version=16, captured_uptime_ms=424593)
    data.update(changes)
    return json.dumps(dict(event_id=identifier, event_type='roi_occ', data=data))


class RoiTelemetryTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = SimpleNamespace(offline_db=tmp.name + '/outbox.sqlite3',
                                     offline_reserve_bytes=0, offline_reserve_fraction=.01,
                                     offline_max_payload=4096, offline_max_fraction=.5)
        self.store = Outbox(self.config)
        self.addCleanup(self.store.close)

    def test_durable_deduplicated_two_roi_replay(self):
        self.store.put(TOPIC, payload())
        self.store.put(TOPIC, payload())
        other = '20000000-0000-0000-0000-000000000002'
        self.store.put(TOPIC, payload('two', roi_id=other, value=0))
        batch = self.store.batch()
        self.assertEqual(len(batch), 2)
        line = event_to_line(batch[0])
        self.assertIn('roi_occ,branch_id=', line)
        self.assertIn(',roi_id=' + ROI, line)
        self.assertIn(',calibration_version=16', line)
        self.assertIn(' value=63.0 ', line)
        self.assertIn(',roi_id=' + other, event_to_line(batch[1]))
        with patch('selfiq.influx_sync.enviar_influx', side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                self.store.replay_once(lambda rows: send_batch(self.config, rows, lambda *a: 0))
        self.assertEqual(self.store.batch(), batch)
        with patch('selfiq.influx_sync.enviar_influx') as send:
            self.assertEqual(self.store.replay_once(
                lambda rows: send_batch(self.config, rows, lambda *a: 0)), 2)
            self.assertEqual(send.call_args.args[1], '\n'.join(map(event_to_line, batch)))
        self.assertEqual(self.store.batch(), [])

    def test_invalid_roi_events_not_acknowledged_or_persisted(self):
        for change in (dict(roi_id='invalid'), dict(value=True), dict(value=101),
                       dict(value=-1), dict(value=1.5), dict(config_version=0),
                       dict(calibration_version='16'), dict(captured_uptime_ms=-1)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.store.put(TOPIC, payload(**change))
        with self.assertRaises(ValueError):
            self.store.put(TOPIC.replace('shelf_cam', 'aforo'), payload())
        self.assertEqual(self.store.batch(), [])

    def test_generic_people_occupancy_unchanged(self):
        event = dict(topic=TOPIC, event_id='aforo', received_at=100,
                     payload=dict(event_type='occupancy', data=dict(value=5)))
        self.assertTrue(event_to_line(event).startswith('occupancy,'))
        self.assertNotIn('roi_id=', event_to_line(event))

    def test_incompatible_old_event_is_archived_without_starving_new_events(self):
        bad = json.dumps(dict(event_id='legacy', event_type='temperature', data={}))
        self.store.put(TOPIC, bad)
        self.store.put(TOPIC, payload())
        with patch('selfiq.influx_sync.enviar_influx') as send:
            self.store.replay_once(
                lambda batch: send_batch(self.config, batch, lambda *a: 0,
                                        self.store.quarantine))
        self.assertEqual(self.store.batch(), [])
        archived = self.store.db.execute(
            'SELECT payload FROM event_quarantine WHERE event_id=?', ('legacy',)).fetchone()
        self.assertEqual(archived[0], bad)
        self.assertIn('roi_occ,', send.call_args.args[1])
        self.store.put(TOPIC, bad)  # replay must not resurrect the poison event
        self.assertEqual(self.store.batch(), [])
