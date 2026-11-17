"""Contract for production shelf occupancy; not people occupancy."""
import re

UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


def validate_roi_occ(data, module):
    if module != 'shelf_cam' or not isinstance(data, dict):
        raise ValueError('roi_occ requires shelf_cam data')
    if not isinstance(data.get('roi_id'), str) or not UUID.fullmatch(data['roi_id']):
        raise ValueError('Invalid roi_id')
    for name, minimum, maximum in (
        ('value', 0, 100), ('config_version', 1, 4294967295),
        ('calibration_version', 1, 4294967295),
        ('captured_uptime_ms', 1, 9223372036854775807),
    ):
        value = data.get(name)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError('Invalid roi_occ ' + name)
