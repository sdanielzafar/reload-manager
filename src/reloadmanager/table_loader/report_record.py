from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class ReportRecord:
    table: str
    strategy: str
    status: str
    start: float
    end: float
    num_records: int
    error: str

    @property
    def duration(self) -> float:
        return round((self.end - self.start) / 60, 2)

    @staticmethod
    def format_mst(t: float) -> str:
        return datetime.fromtimestamp(t, ZoneInfo("America/Phoenix")).strftime('%-m/%-d/%y %-I:%M %p')

    def __str__(self):
        return f"{self.table},{self.strategy},{self.status},{self.format_mst(self.start)},{self.format_mst(self.end)},"\
               f"{self.duration:.2f},{self.num_records},{self.error.strip()}\n"
