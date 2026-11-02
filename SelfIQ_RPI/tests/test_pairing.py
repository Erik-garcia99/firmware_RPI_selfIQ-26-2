import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from selfiq.controller import Controller
from selfiq.models import Credentials


class PairingTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000
        self.network = Mock()
        self.network.ap_healthy.return_value = True
        self.network.run.return_value = 'connected'
        self.config = SimpleNamespace(sta_interface='wlan0', idle_seconds=300,
                                      state_file='unused', virtual_ap=False)
        self.ctrl = Controller(self.config, self.network, clock=lambda: self.now)
        self.ctrl.online = True
        self.ctrl.creds = Credentials('shop', '12345678')

    def test_recovery_ap_does_not_distribute_credentials(self):
        self.ctrl.ap_wanted = self.ctrl.recovery_ap = True
        self.assertIsNone(self.ctrl.requested_credentials())

    def test_explicit_pairing_expires_even_if_clients_keep_requesting(self):
        self.ctrl.reopen()
        self.assertEqual(self.ctrl.requested_credentials(), self.ctrl.creds)
        self.now += 119
        self.assertEqual(self.ctrl.requested_credentials(), self.ctrl.creds)
        self.now += 2
        self.assertIsNone(self.ctrl.requested_credentials())
        self.ctrl.tick()
        self.network.stop_ap.assert_called_once()
        self.assertFalse(self.ctrl.ap_wanted)

    def test_stop_prevents_further_credential_delivery(self):
        self.ctrl.reopen()
        self.ctrl.stop_provisioning()
        self.assertIsNone(self.ctrl.requested_credentials())

    def test_wifi_configuration_does_not_open_pairing(self):
        self.ctrl._sta_connected = lambda: False
        self.network.activate.return_value = True
        with patch('selfiq.controller.write_state'):
            self.ctrl.configure(self.ctrl.creds)
        self.assertFalse(self.ctrl.ap_wanted)
        self.assertIsNone(self.ctrl.requested_credentials())
        self.network.start_ap.assert_not_called()
