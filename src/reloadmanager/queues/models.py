from dataclasses import dataclass, fields


@dataclass()
class QueueRecord:
    source_table: str
    target_table: str
    where_clause: str
    primary_key: str
    event_time: str
    trigger_time: str | None
    strategy: str
    lock_rows: bool
    status: str
    priority: int
    event_time_latest: str | None

    def to_sql_values(self) -> str:
        def lit(f, v):
            # if f.name == "where_clause" and v is not None:
            #     return v  # Don't quote SQL clauses
            match v:
                case None:
                    return "NULL"
                case bool() as b:
                    return "true" if b else "false"
                case int():
                    return str(v)
                case str() as s:
                    s_rep: str = s.replace("'", "''")  # or replace("'", "''") for SQL escaping
                    return f"'{s_rep}'"

        return "(" + ", ".join(lit(f, v) for f, v in zip(fields(self), iter(self))) + ")"

    @classmethod
    def fields_str(cls) -> str:
        return ", ".join(f.name for f in fields(cls))

    def __getitem__(self, index):
        f = (
            self.source_table,
            self.target_table,
            self.where_clause,
            self.primary_key,
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
        return 11

    def __iter__(self):
        # re-use the existing tuple to avoid dup logic
        return iter(self[i] for i in range(len(self)))
