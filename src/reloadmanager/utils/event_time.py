# from datetime import datetime
# from zoneinfo import ZoneInfo
#
#
# class EventTime(int):
#     __slots__ = ("_tz",)
#     _default_tz: ZoneInfo = ZoneInfo("UTC")
#
#     @classmethod
#     def set_timezone(cls, tz_str: str):
#         cls._default_tz = ZoneInfo(tz_str)
#
#     def __new__(cls, ts_str: str | None):
#         if ts_str is None:
#             raise ValueError("None is not a valid EventTime")
#         cls._validate_dt_str(ts_str)
#         dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
#         dt = dt.replace(tzinfo=cls._default_tz)
#         epoch = int(dt.timestamp())
#         obj = super().__new__(cls, epoch)
#         obj._tz = cls._default_tz
#         return obj
#
#     def _validate_dt_str(dt_string: str) -> str:
#         try:
#             datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
#             return dt_string
#         except ValueError:
#             raise ValueError(f"Datetime passed '{dt_string}' is incorrect. Must match format '%Y-%m-%d %H:%M:%S'")
#
#     def __str__(self):
#         return datetime.fromtimestamp(int(self), tz=self._tz).strftime("%Y-%m-%d %H:%M:%S")
#
#     def to_datetime(self) -> datetime:
#         return datetime.fromtimestamp(int(self), tz=self._tz)
#
#     @classmethod
#     def now(cls):
#         dt = datetime.now(tz=cls._default_tz).strftime("%Y-%m-%d %H:%M:%S")
#         return cls(dt)
#
#     @classmethod
#     def from_epoch(cls, epoch: int):
#         obj = super().__new__(cls, epoch)
#         obj._tz = cls._default_tz
#         return obj



from datetime import datetime
from zoneinfo import ZoneInfo

_FMT = "%Y-%m-%d %H:%M:%S"


class EventTime(int):
    """
    Epoch-seconds wrapper that remembers the default zone
    and turns YYYY-MM-DD HH:MM:SS strings into ints.
    """

    _default_tz: ZoneInfo = ZoneInfo("UTC")   # global knob

    @classmethod
    def set_timezone(cls, tz_str: str | ZoneInfo) -> None:
        """Change the canonical timezone for all future EventTime objects."""
        cls._default_tz = ZoneInfo(tz_str) if isinstance(tz_str, str) else tz_str

    def __new__(cls, ts_str: str | None) -> "EventTime":
        if ts_str is None:
            raise ValueError("None is not a valid EventTime")
        cls._validate(ts_str)

        dt = datetime.strptime(ts_str, _FMT).replace(tzinfo=cls._default_tz)
        epoch = int(dt.timestamp())
        return super().__new__(cls, epoch)

    @classmethod
    def from_datetime_local(cls, dt: datetime) -> "EventTime":
        return super().__new__(cls, int(dt.replace(tzinfo=cls._default_tz).timestamp()))

    @classmethod
    def from_datetime(cls, dt: datetime) -> "EventTime":
        return super().__new__(cls, int(dt.replace(tzinfo=cls._default_tz).timestamp()))

    @classmethod
    def from_epoch(cls, epoch: int) -> "EventTime":
        return super().__new__(cls, epoch)

    @classmethod
    def now(cls) -> "EventTime":
        stamp = datetime.now(tz=cls._default_tz).strftime(_FMT)
        return cls(stamp)

    @staticmethod
    def _validate(dt_string: str) -> None:
        try:
            datetime.strptime(dt_string, _FMT)
        except ValueError as e:
            raise ValueError(
                f"'{dt_string}' is not '{_FMT}'"
            ) from e

    def to_datetime(self, *, tz: ZoneInfo | None = None) -> datetime:
        """Return a tz-aware datetime (defaulting to the class’s zone)."""
        return datetime.fromtimestamp(int(self), tz or self._default_tz)

    def __str__(self) -> str:              # str(et)
        return datetime.fromtimestamp(int(self), tz=type(self)._default_tz).strftime("%Y-%m-%d %H:%M:%S")

    def __repr__(self) -> str:             # inspect(et)
        return f"EventTime('{self}')"
