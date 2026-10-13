#!/usr/bin/env python3
"""Run on the Raspberry as root; never prints branch/admin tokens."""
import getpass
import json
import urllib.request
import urllib.error
from pathlib import Path


def main():
    config = json.loads(Path('/etc/selfiq/config.json').read_text())
    token = getpass.getpass('Token de sucursal: ').strip()
    request = urllib.request.Request('http://127.0.0.1/admin/backend/configure',
        data=json.dumps({'branch_token':token}).encode(),
        headers={'Content-Type':'application/json','X-SelfIQ-Token':config['admin_token']},method='POST')
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.load(response)
        print(result['message'])
        print('Gateway ID:', result['gateway_id'])
        print('Compruebe el registro: sudo journalctl -u selfiq -f')
    except urllib.error.HTTPError as exc:
        try:
            print('Error:', json.load(exc).get('detail', 'No pudo guardarse la configuración'))
        except (ValueError, AttributeError):
            print('Error HTTP:', exc.code)
        raise SystemExit(1)
    except urllib.error.URLError:
        raise SystemExit('El servicio SelfIQ no responde; revise sudo systemctl status selfiq')


if __name__ == '__main__':
    main()