from dataclasses import dataclass, fields


@dataclass()
class QueueRecord:
    source_table: str
    target_table: str
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
        return 9

    def __iter__(self):
        # re-use the existing tuple to avoid dup logic
        return iter(self[i] for i in range(len(self)))


@dataclass(frozen=True)
class TableAttrRecord:
    source_table: str
    target_table: str
    strategy: str
    disabled: bool
    priority: int
    min_staleness: int
    max_staleness: int

    @classmethod
    def from_tuple(cls, line: tuple):
        if len(line) != len(fields(cls)):
            raise ValueError(f"Input line {line} should have {len(fields(cls))} fields")

        source_table, target_table, strategy, disabled, priority, min_staleness, max_staleness = line

        def valid_table(s: str) -> str | None:
            if s:
                if len(s.split(".")) != 2:
                    raise ValueError(f"Table '{s}' must have 2 namespaces in the input config file")
                return s
            return None

        if strategy not in ["TPT", "WriteNOS", "JDBC"]:
            raise ValueError(f"Input line: {line} has invalid method. Should be 'TPT', 'WriteNOS', or 'JDBC'")

        if isinstance(disabled, str):
            if disabled.strip().lower() not in ["true", "false"]:
                raise ValueError(f"Input line: {line} has invalid disabled status. Should be 'true' or 'false'")
            disabled = disabled.strip().lower() == "true"

        return cls(
            valid_table(source_table),
            valid_table(target_table or source_table),
            strategy,
            disabled,
            int(priority),
            int(min_staleness or 0),
            int(max_staleness)
        )