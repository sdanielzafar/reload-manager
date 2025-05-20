from reloadmanager.clients.teradata_client import TeradataClient


class TeradataInterface(TeradataClient):
    def __init__(self, source_table: str, lock_rows: bool = True):
        super().__init__()
        self.db, self.table = source_table.split(".")
        self.lock_rows = lock_rows

    def safe_query(self, query: str, headers: bool = True) -> list[dict] | list[tuple]:
        return self.query(f'''{"LOCKING ROW FOR ACCESS" if self.lock_rows else ""}
        {query};''', headers=headers)

    def get_row_count(self) -> list[dict]:
        return self.safe_query(
            f"(SELECT CAST(COUNT(*) AS BIGINT) as row_count FROM '{self.db}.{self.table}')"
        )

    def get_columns(self) -> list[dict]:
        return self.safe_query(
            # f"(SELECT * FROM DBC.ColumnsV WHERE DatabaseName = '{self.db}' and TableName = '{self.table}')"
            f"HELP COLUMN {self.db}.{self.table}"
        )

    def get_indexes(self) -> list[dict]:
        return self.safe_query(
            f"(SELECT * FROM DBC.IndicesV WHERE DatabaseName = '{self.db}' and TableName = '{self.table}')"
        )
