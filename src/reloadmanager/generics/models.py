from dataclasses import dataclass


@dataclass
class TableInfo:
    catalog: str | None
    schema: str
    table: str

    def as_path(self):
        return "__".join(v for v in (self.catalog, self.schema, self.table) if v)

    def __str__(self):
        return ".".join(v for v in (self.catalog, self.schema, self.table) if v)
