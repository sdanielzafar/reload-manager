from dataclasses import dataclass


@dataclass
class TableInfo:
    catalog: str | None
    schema: str
    table: str

    def as_path(self):
        return "__".join(v for v in (self.catalog, self.schema, self.table) if v)

    @property
    def catalog_schema(self):
        return ".".join(v for v in (self.catalog, self.schema) if v)

    def __str__(self):
        return ".".join(v for v in (self.catalog, self.schema, self.table) if v)
