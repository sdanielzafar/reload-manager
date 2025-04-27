from datetime import datetime
from zoneinfo import ZoneInfo


class EventTime(int):
    __slots__ = ("_tz",)
    _default_tz: ZoneInfo = ZoneInfo("UTC")

    @classmethod
    def set_timezone(cls, tz_str: str):
        cls._default_tz = ZoneInfo(tz_str)

    def __new__(cls, ts_str: str | None):
        if ts_str is None:
            raise ValueError("None is not a valid EventTime")
        cls._validate_dt_str(ts_str)
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=cls._default_tz)
        epoch = int(dt.timestamp())
        obj = super().__new__(cls, epoch)
        obj._tz = cls._default_tz
        return obj

    def _validate_dt_str(dt_string: str) -> str:
        try:
            datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
            return dt_string
        except ValueError:
            raise ValueError(f"Datetime passed '{dt_string}' is incorrect. Must match format '%Y-%m-%d %H:%M:%S'")

    def __str__(self):
        return datetime.fromtimestamp(int(self), tz=self._tz).strftime("%Y-%m-%d %H:%M:%S")

    def to_datetime(self) -> datetime:
        return datetime.fromtimestamp(int(self), tz=self._tz)

    @classmethod
    def now(cls):
        dt = datetime.now(tz=cls._default_tz).strftime("%Y-%m-%d %H:%M:%S")
        return cls(dt)

    @classmethod
    def from_epoch(cls, epoch: int):
        obj = super().__new__(cls, epoch)
        obj._tz = cls._default_tz
        return obj
