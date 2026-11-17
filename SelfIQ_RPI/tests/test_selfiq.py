import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from selfiq.models import Credentials, atomic_json
from test_support import make_config as Config
from selfiq.protocol import *
from selfiq.controller import Controller
from selfiq.credential_integrity import read_state

class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_and_coalesced_frames(self):
        reader = asyncio.StreamReader()
        request = encode(HEADER, 0, 1, b'\x00')
        task = asyncio.create_task(read_frame(reader))
        for byte in request[:4]:
            reader.feed_data(bytes([byte]))
            await asyncio.sleep(0)
        self.assertFalse(task.done())
        reader.feed_data(request[4:] + request)
        self.assertEqual(await task, Frame(HEADER, 0, 1, b'\x00'))
        self.assertEqual(await read_frame(reader), Frame(HEADER, 0, 1, b'\x00'))

    async def test_nack_sentinel_not_255_bytes(self):
        r = asyncio.StreamReader()
        r.feed_data(NACK)
        self.assertTrue((await read_frame(r)).nack)

    async def test_invalid_length_header_and_truncated(self):
        for packet in (b'\xab\xcd\x00', b'\xab\xcd\xa2', b'\x00\x00\x02', b'\x54\x33\xff\x01\x00'):
            r = asyncio.StreamReader()
            r.feed_data(packet)
            r.feed_eof()
            with self.assertRaises(ValueError):
                await read_frame(r)
        r = asyncio.StreamReader()
        r.feed_data(b'\xab\xcd\x02\x01')
        r.feed_eof()
        with self.assertRaises(asyncio.IncompleteReadError):
            await read_frame(r)

    async def test_timeout(self):
        with self.assertRaises(asyncio.TimeoutError):
            await read_frame(asyncio.StreamReader(), .01)

    async def test_credentials_wire(self):
        c = Credentials('Casa', '12345678').validate()
        self.assertEqual(credentials_frame(c), bytes.fromhex('5433111269044361736170083132333435363738'))
        c = Credentials('S'*32, 'P'*63).validate()
        packet = credentials_frame(c)
        self.assertEqual(len(packet), 103)
        self.assertEqual(packet[3], 0x12)
        self.assertEqual(packet[4:], tlv(SSID, c.ssid) + tlv(PASSWORD, c.password))

    async def test_enterprise_credentials_are_rejected(self):
        with self.assertRaises(ValueError):
            Credentials('S'*32, 'P'*64, 2, 'U'*31).validate()

class FakeNetwork:
    def __init__(self):
        self.ap_active = False
        self.current = None
        self.accept = True
        self.ap_fails = False
        self.profiles = set()
    def stop_ap(self): self.ap_active = False
    def start_ap(self, connected=False):
        if self.ap_fails: raise RuntimeError('AP falló')
        self.ap_active = True
    def ap_healthy(self): return self.ap_active
    def active(self, name): return self.current == name
    def profile_exists(self, name): return name in self.profiles
    def activate(self, name):
        self.current = name if self.accept else None
        return self.accept
    def candidate(self, name, c): self.profiles.add(name)
    def disconnect_sta(self): self.current = None
    def enable_autoconnect(self, name): pass
    def ap_clients(self): return 0
    def run(self, *args, **kwargs):
        if not self.current:
            return '--'
        return self.current if 'GENERAL.CONNECTION' in args else '192.168.1.5/24'

class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.c = replace(Config(), state_file=self.tmp.name + '/creds.json')
        self.now = 1000
        self.net = FakeNetwork()
        self.ctrl = Controller(self.c, self.net, clock=lambda: self.now)
        self.creds = Credentials('Casa', '12345678')
    def test_first_boot_and_no_idle_close_before_credentials(self):
        self.ctrl.bootstrap()
        self.assertTrue(self.net.ap_active)
        self.now += 1000
        self.ctrl.tick()
        self.assertTrue(self.net.ap_active)
        self.assertFalse(self.ctrl.can_distribute())
    def test_success_persist_reboot_and_idle(self):
        self.ctrl.bootstrap()
        self.ctrl.configure(self.creds)
        # Saving STA no longer opens enrollment; only an explicit search does.
        self.assertFalse(self.net.ap_active)
        self.assertFalse(self.ctrl.can_distribute())
        self.ctrl.reopen()
        self.assertTrue(self.ctrl.can_distribute())
        self.assertEqual(read_state(self.c.state_file)['credentials']['ssid'], 'Casa')
        self.now += 119
        self.ctrl.tick()
        self.assertTrue(self.net.ap_active)
        self.now += 1
        self.ctrl.tick()
        self.assertFalse(self.net.ap_active)
        reboot = Controller(self.c, self.net)
        reboot.bootstrap()
        self.assertTrue(reboot.online)
        self.assertFalse(self.net.ap_active)
    def test_activity_updates_idle_but_does_not_extend_pairing_window(self):
        self.ctrl.configure(self.creds)
        self.ctrl.reopen()
        deadline = self.ctrl.pairing_until
        self.now += 60
        self.assertEqual(self.ctrl.requested_credentials(), self.creds)
        self.assertEqual(self.ctrl.last_request, self.now)
        # Activity does not extend the fixed 120 s enrollment window.
        self.assertEqual(self.ctrl.pairing_until, deadline)
        self.now += 59
        self.ctrl.tick()
        self.assertTrue(self.net.ap_active)
        self.now += 2
        self.ctrl.tick()
        self.ctrl.reopen()
        self.assertTrue(self.net.ap_active)
    def test_failed_candidate_not_persisted(self):
        self.ctrl.bootstrap()
        self.net.accept = False
        with self.assertRaises(RuntimeError):
            self.ctrl.configure(self.creds)
        self.assertFalse(Path(self.c.state_file).exists())
        self.assertFalse(self.ctrl.can_distribute())
        # AP retries are rate-limited after the bootstrap attempt.
        self.now += 30
        self.ctrl.tick()
        self.assertTrue(self.net.ap_active)
        self.assertFalse(self.ctrl.busy)
    def test_failed_replacement_preserves_saved_credentials(self):
        self.ctrl.configure(self.creds)
        original = Path(self.c.state_file).read_bytes()
        self.net.accept = False
        self.net.current = None
        with self.assertRaises(RuntimeError):
            self.ctrl.configure(Credentials('Otro', 'abcdefgh'))
        self.assertEqual(Path(self.c.state_file).read_bytes(), original)
        self.assertEqual(self.ctrl.creds.ssid, 'Casa')
    def test_active_sta_cannot_be_replaced(self):
        self.ctrl.configure(self.creds)
        saved = Path(self.c.state_file).read_bytes()
        profile = self.net.current
        profiles = self.net.profiles.copy()
        with self.assertRaisesRegex(RuntimeError, 'STA ya conectada'):
            self.ctrl.configure(Credentials('Otro', 'abcdefgh'))
        self.assertEqual(self.net.current, profile)
        self.assertEqual(self.net.profiles, profiles)
        self.assertEqual(Path(self.c.state_file).read_bytes(), saved)
    def test_sta_loss_reopens_and_retries(self):
        self.ctrl.configure(self.creds)
        self.net.current = None
        self.net.accept = False
        self.ctrl.tick()
        self.assertFalse(self.ctrl.online)
        self.assertFalse(self.net.ap_active)
        self.now += self.c.offline_ap_delay
        self.ctrl.tick()
        self.assertTrue(self.net.ap_active)
        self.net.accept = True
        self.now += 61
        self.ctrl.tick()
        self.assertTrue(self.ctrl.online)
    def test_ap_failure_does_not_discard_verified_sta(self):
        self.net.ap_fails = True
        self.ctrl.configure(self.creds)
        self.assertTrue(self.ctrl.online)
        self.assertFalse(self.ctrl.can_distribute())
        self.assertTrue(Path(self.c.state_file).exists())

    def test_reopen_preserves_file_profile_and_sta_then_expires(self):
        self.ctrl.configure(self.creds)
        saved = Path(self.c.state_file).read_bytes()
        profile = self.ctrl.profile
        profiles = self.net.profiles.copy()
        self.ctrl.stop_provisioning()
        self.ctrl.reopen()
        self.assertEqual(Path(self.c.state_file).read_bytes(), saved)
        self.assertEqual(self.ctrl.profile, profile)
        self.assertEqual(self.net.profiles, profiles)
        self.assertEqual(self.net.current, profile)
        self.assertFalse(self.ctrl.configuration_required())
        self.now += 300
        self.ctrl.tick()
        self.assertFalse(self.net.ap_active)
        self.assertTrue(self.ctrl.online)
        self.assertEqual(Path(self.c.state_file).read_bytes(), saved)

    def test_reopen_without_connected_saved_wifi_is_rejected(self):
        self.ctrl.bootstrap()
        with self.assertRaises(ValueError):
            self.ctrl.reopen()
        self.ctrl.configure(self.creds)
        self.net.current = None
        with self.assertRaises(ValueError):
            self.ctrl.reopen()

class ModelTests(unittest.TestCase):
    def test_test_settings_have_isolated_storage(self):
        first, second = Config(), Config()
        for field in ('offline_db', 'state_file', 'gateway_state_file', 'runtime_dir'):
            self.assertNotEqual(getattr(first, field), getattr(second, field))

    def test_byte_limits_and_injection(self):
        for c in (Credentials('á'*17, '12345678'), Credentials('bad\nssid', '12345678'), Credentials('Casa', 'short'), Credentials('S', 'P', 2, 'U'*32), Credentials('S', 'P', 3)):
            with self.assertRaises(ValueError): c.validate()
    def test_atomic_file_private(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'state.json'
            atomic_json(p, {'v': 1})
            atomic_json(p, {'v': 2})
            self.assertEqual(json.loads(p.read_text()), {'v': 2})
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)

if __name__ == '__main__':
    unittest.main()
