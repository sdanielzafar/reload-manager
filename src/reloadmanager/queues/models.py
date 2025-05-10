from dataclasses import dataclass, fields


@dataclass()
class QueueRecord:
    source_table: str
    target_table: str
    where_clause: str
    event_time: str
    trigger_time: str | None
    strategy: str
    lock_rows: bool
    status: str
    priority: int
    event_time_latest: str | None

    def to_sql_values(self) -> str:
        def lit(v):
            match v:
                case None:              # NULL sentinel
                    return "NULL"
                case bool() as b:       # booleans → true/false
                    return "true" if b else "false"
                case int():             # numerics stay raw
                    return str(v)
                case str() as s:        # strings: escape quotes
                    s_rep: str = s.replace("'", "")
                    return f"'{s_rep}'"

        return "(" + ", ".join(lit(v) for v in iter(self)) + ")"

    @classmethod
    def fields_str(cls) -> str:
        return ", ".join(f.name for f in fields(cls))

    def __getitem__(self, index):
        f = (
            self.source_table,
            self.target_table,
            self.where_clause,
            self.event_time,
            self.trigger_time,
            self.strategy,
            self.lock_rows,
            self.status,
            self.priority,
            self.event_time_latest
        )
        return f[index]

    def __len__(self):
        return 10

    def __iter__(self):
        # re-use the existing tuple to avoid dup logic
        return iter(self[i] for i in range(len(self)))
