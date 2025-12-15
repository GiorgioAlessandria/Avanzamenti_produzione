from freezegun import freeze_time
from zoneinfo import ZoneInfo
from sync_input import seconds_until_next_allowed

TZ = ZoneInfo("Europe/Rome")
ALLOWED = {0, 1, 2, 3, 4, 5}


# attenzione: timezone gestita in base a come lo usi
@freeze_time("2025-12-12 12:00:00")
def test_next_window_freeze():
    s = seconds_until_next_allowed(6, 18, ALLOWED, tz=TZ)
    assert s == 11 * 3600
