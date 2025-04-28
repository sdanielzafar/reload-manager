import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from reloadmanager.utils.event_time import EventTime
EventTime.set_timezone("America/Phoenix")

PHOENIX = ZoneInfo("America/Phoenix")


def test_event_time_creation():
    ts = "2025-03-18 05:26:55"
    et = EventTime(ts)
    expected = int(datetime(2025, 3, 18, 5, 26, 55, tzinfo=PHOENIX).timestamp())
    assert isinstance(et, EventTime)
    assert int(et) == expected


def test_event_time_str_roundtrip():
    ts = "2025-03-18 05:26:55"
    et = EventTime(ts)
    assert str(et) == ts


def test_event_time_to_datetime():
    ts = "2025-03-18 05:26:55"
    et = EventTime(ts)
    dt = et.to_datetime()
    assert dt == datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=PHOENIX)


def test_event_time_from_epoch():
    epoch = 1742297215  # 2025-03-18 04:26:55 Phoenix time
    et = EventTime.from_epoch(epoch)
    assert isinstance(et, EventTime)
    assert int(et) == epoch
    assert str(et) == "2025-03-18 04:26:55"


def test_event_time_now(monkeypatch):
    fake_now = datetime(2030, 1, 1, 12, 0, 0, tzinfo=PHOENIX)

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fake_now

    monkeypatch.setattr("reloadmanager.utils.event_time.datetime", FakeDatetime)

    et_now = EventTime.now()
    assert str(et_now) == "2030-01-01 12:00:00"


def test_event_time_eq():
    et1 = EventTime("2025-03-18 05:26:55")
    et2 = EventTime("2025-03-18 05:26:55")
    assert et1 == et2


def test_event_time_malformat():
    with pytest.raises(ValueError) as exc_info:
        EventTime("2025-03-18T05:26:55Z")

    # flex on the error message contents
    assert "'2025-03-18T05:26:55Z' is not '%Y-%m-%d %H:%M:%S'" in str(exc_info.value)


def test_event_time_lt_gt():
    et1 = EventTime("2025-03-18 05:26:55")
    et2 = EventTime("2025-03-18 05:26:56")
    assert et1 < et2
    assert et2 > et1


def test_event_time_le_ge():
    et1 = EventTime("2025-03-18 05:26:55")
    et2 = EventTime("2025-03-18 05:26:55")
    et3 = EventTime("2025-03-18 05:26:56")

    assert et1 <= et2
    assert et1 <= et3
    assert et3 >= et1
    assert et2 >= et1


def test_event_time_compare_to_int():
    et = EventTime("2025-03-18 05:26:55")
    epoch = int(datetime(2025, 3, 18, 5, 26, 55, tzinfo=PHOENIX).timestamp())
    assert et == epoch
    assert et <= epoch
    assert et >= epoch
