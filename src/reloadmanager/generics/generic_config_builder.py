from abc import ABC

from reloadmanager.generics.models import TableInfo


class GenericConfigBuilder(ABC):

    def __init__(
            self,
            source_table: str,
            target_table: str,
            where_clause: str = None,
            lock_rows: bool = True
    ):
        self.source_table: TableInfo = self._validate_source_table(source_table)
        self.target_table: TableInfo = self._validate_target_table(target_table)
        self.where_clause: str = where_clause
        self.lock_rows: bool = lock_rows

    @staticmethod
    def _validate_source_table(source_table: str) -> TableInfo:
        source_schema_table: list[str] = source_table.upper().split(".")
        if len(source_schema_table) != 2:
            raise Exception(f"Argument source_table must have format schema.table. Not '{source_table}'")
        schema, table = source_schema_table
        return TableInfo(catalog=None, schema=schema, table=table)

    @staticmethod
    def _validate_target_table(target_table: str) -> TableInfo:
        source_schema_table: list[str] = target_table.upper().split(".")
        if len(source_schema_table) != 3:
            raise Exception(f"Argument target_table must have format catalog.schema.table. Not '{target_table}'")
        return TableInfo(*source_schema_table)