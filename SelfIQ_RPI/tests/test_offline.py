import asyncio
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from selfiq.models import Config, Credentials
from selfiq.controller import Controller
from selfiq.credential_integrity import read_state, write_state
from selfiq.outbox import Outbox, StorageFull
from selfiq.app import TCPService, create_app
from selfiq.mqtt_bridge import MQTTBridge

TOPIC = 'selfiq/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/led/telemetry'
def payload(i='one', value=1):
    return json.dumps({'event_id':i, 'event_type':'temperature','data':{'value':value}}).encode()

class FakeNet:
    def __init__(self):
        self.connected = True
        self.ap = False
        self.clients = 1
        self.attempts = 0
        self.recover = False
    def run(self, *args, **kwargs):
        return ('selfiq-sta-a' if 'GENERAL.CONNECTION' in args else '192.168.1.5/24') if self.connected else '--'
    def active(self, name): return self.connected
    def profile_exists(self, name): return True
    def activate(self, name):
        self.attempts += 1
        self.connected = self.recover
        return self.connected
    def enable_autoconnect(self, name): pass
    def ap_healthy(self): return self.ap
    def start_ap(self, connected=False): self.ap = True
    def stop_ap(self): self.ap = False
    def ap_clients(self): return self.clients

class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.c = Config(offline_db=self.tmp.name+'/outbox.sqlite3',
                        state_file=self.tmp.name+'/credentials.json',
                        offline_reserve_bytes=0, offline_reserve_fraction=.01,
                        gateway_state_file=self.tmp.name+'/gateway.json')
        self.store = Outbox(self.c)
        self.addCleanup(self.store.close)
    def test_commit_survives_second_connection(self):
        self.store.put(TOPIC,payload())
        other = Outbox(self.c)
        try: self.assertEqual(other.batch()[0]['event_id'],'one')
        finally: other.close()
    def test_process_exit_after_commit(self):
        source = '''from selfiq.models import Config
from selfiq.outbox import Outbox
import os
c=Config(offline_db=%r, offline_reserve_bytes=0,offline_reserve_fraction=.01)
o=Outbox(c)
o.put(%r,%r)
os._exit(0)
''' % (self.c.offline_db, TOPIC, payload('crash'))
        subprocess.run([sys.executable,'-c',source],check=True)
        self.assertEqual(self.store.batch()[0]['event_id'],'crash')
    def test_duplicate_and_conflict(self):
        self.store.put(TOPIC,payload())
        self.store.put(TOPIC,payload())
        self.assertEqual(self.store.status()['pending'],1)
        with self.assertRaises(ValueError): self.store.put(TOPIC,payload(value=2))
    def test_reserve_rejects_without_deleting(self):
        self.store.put(TOPIC,payload())
        self.store.disk_usage=lambda _: SimpleNamespace(total=10**9,free=0)
        with self.assertRaises(StorageFull): self.store.put(TOPIC,payload('two'))
        self.assertEqual(self.store.status()['pending'],1)
        self.assertEqual(self.store.put(TOPIC,payload()), 'one')
    def test_capacity_quota(self):
        self.store.disk_usage=lambda _: SimpleNamespace(total=100000,free=100000)
        with self.assertRaises(StorageFull): self.store.put(TOPIC,payload())
    def test_lost_cloud_reply_retains_data(self):
        self.store.put(TOPIC,payload())
        def fail(batch): raise TimeoutError()
        with self.assertRaises(TimeoutError): self.store.replay_once(fail)
        self.assertEqual(self.store.status()['pending'],1)
    def test_partial_receipt_and_concurrent_ingestion(self):
        self.store.put(TOPIC,payload())
        self.store.put(TOPIC,payload('two'))
        def accept(batch):
            self.store.put(TOPIC,payload('three'))
            return ['one']
        self.assertEqual(self.store.replay_once(accept),1)
        self.assertEqual([x['event_id'] for x in self.store.batch()], ['two','three'])
    def test_invalid_cloud_receipt_does_not_delete(self):
        self.store.put(TOPIC,payload())
        with self.assertRaises(ValueError): self.store.replay_once(lambda _: ['unoffered'])
        self.assertEqual(self.store.status()['pending'],1)
    def test_corrupt_credentials(self):
        data={'profile':'selfiq-sta-a','credentials':{'ssid':'test','password':'password'}}
        write_state(self.c.state_file,data)
        self.assertEqual(read_state(self.c.state_file),data)
        stored=json.loads(Path(self.c.state_file).read_text())
        stored['data']['credentials']['password']='changed!'
        Path(self.c.state_file).write_text(json.dumps(stored))
        with self.assertRaises(ValueError): read_state(self.c.state_file)
    def controller(self):
        net=FakeNet(); now=[100.0]
        ctrl=Controller(self.c,net,clock=lambda:now[0])
        ctrl.creds=Credentials('test','password'); ctrl.profile='selfiq-sta-a'
        ctrl.ap_wanted=False
        return ctrl,net,now
    def test_wifi_loss_opens_ap(self):
        ctrl,net,now=self.controller()
        net.connected=False; ctrl.tick()
        now[0]+=31; ctrl.tick()
        self.assertTrue(net.ap); self.assertTrue(ctrl.recovery_ap)
    def test_cloud_failure_does_not_change_wifi(self):
        ctrl,net,now=self.controller(); ctrl.tick()
        self.assertEqual(net.attempts,0); self.assertFalse(net.ap)
    def test_recovery_keeps_ap_while_clients_present(self):
        ctrl,net,now=self.controller()
        ctrl.recovery_ap=ctrl.ap_wanted=net.ap=True
        ctrl.tick(); now[0]+=1000; ctrl.tick()
        self.assertTrue(net.ap)
        net.clients=0; ctrl.tick(); now[0]+=301; ctrl.tick()
        self.assertFalse(net.ap)
    def test_retry_budget_changes_interval(self):
        ctrl,net,now=self.controller(); net.connected=False
        for _ in range(20): ctrl.tick(); now[0]+=60
        self.assertEqual(net.attempts,20)
        ctrl.tick(); self.assertEqual(net.attempts,20)
        now[0]+=300; ctrl.tick(); self.assertEqual(net.attempts,21)
    def test_no_credentials_still_opens_ap(self):
        ctrl,net,now=self.controller(); ctrl.creds=None; ctrl.profile=None
        net.connected=False; ctrl.tick(); now[0]+=31; ctrl.tick()
        self.assertTrue(net.ap)
    def test_bridge_receipt_after_commit(self):
        bridge=MQTTBridge(self.c,SimpleNamespace())
        bridge.outbox=self.store
        client=Mock()
        def receipt(*args,**kwargs):
            self.assertEqual(self.store.status()['pending'],1)
            self.assertEqual(args[1],'one')
        client.publish.side_effect=receipt
        bridge._on_message(client,None,SimpleNamespace(topic=TOPIC,payload=payload(),retain=False))
        client.publish.assert_called_once()
    def test_storage_full_no_receipt(self):
        bridge=MQTTBridge(self.c,SimpleNamespace()); bridge.outbox=self.store
        self.store.disk_usage=lambda _: SimpleNamespace(total=10**9,free=0)
        client=Mock()
        bridge._on_message(client,None,SimpleNamespace(topic=TOPIC,payload=payload(),retain=False))
        client.publish.assert_not_called()
        self.assertEqual(bridge.storage_error,'STORAGE_FULL')
    def test_malformed_event_no_receipt(self):
        bridge=MQTTBridge(self.c,SimpleNamespace()); bridge.outbox=self.store; client=Mock()
        for bad in [b'[]',b'{',payload().replace(b'1}',b'NaN}')]:
            bridge._on_message(client,None,SimpleNamespace(topic=TOPIC,payload=bad,retain=False))
        client.publish.assert_not_called()
    def test_app_constructs_without_network_mutation(self):
        net=FakeNet(); app=create_app(self.c,net)
        self.assertIsNotNone(app.state.mqtt_bridge)
        self.assertEqual(net.attempts,0)
    def test_tcp_status_and_offline_credentials(self):
        async def scenario():
            ctrl,net,now=self.controller()
            ctrl.ap_wanted=ctrl.recovery_ap=net.ap=True; ctrl.online=False
            service=TCPService(ctrl); service.health=lambda:(True,False,True)
            server=await asyncio.start_server(service.handle,'127.0.0.1',0)
            try:
                port=server.sockets[0].getsockname()[1]
                reader,writer=await asyncio.open_connection('127.0.0.1',port)
                # fragmented input
                writer.write(bytes.fromhex('ab cd')); await writer.drain()
                writer.write(bytes.fromhex('01 41')); await writer.drain()
                reply=await asyncio.wait_for(reader.readexactly(10),2)
                self.assertEqual(reply,bytes.fromhex('54 33 07 42 01 00 00 01 01 01'))
                writer.write(bytes.fromhex('ab cd 01 11')); await writer.drain()
                self.assertEqual(await reader.readexactly(5),bytes.fromhex('54 33 ff ff 00'))
                ctrl.online=True
                writer.write(bytes.fromhex('ab cd 01 41')); await writer.drain()
                self.assertEqual((await reader.readexactly(10))[-1],2)
                writer.close(); await writer.wait_closed()
            finally:
                server.close(); await server.wait_closed(); await service.close()
        asyncio.run(scenario())

if __name__=='__main__': unittest.main()
