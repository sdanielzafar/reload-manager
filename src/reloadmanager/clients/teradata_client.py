import teradatasql
from contextlib import contextmanager

from reloadmanager.clients.generic_database_client import GenericDatabaseClient
from reloadmanager.mixins.secret_mixin import SecretMixin


class TeradataClient(GenericDatabaseClient, SecretMixin):
    def __init__(self, logmech: str = "TD2"):
        super().__init__()
        # hydrate env vars, then fetch creds (works on Databricks & local)
        self.td_host: str = self.get_secret("TD_HOST", "reloadmanager")
        self.td_user: str = self.get_secret("TD_USER", "reloadmanager")
        self.td_pass: str = self.get_secret("TD_PASS", "reloadmanager")

        self._conn_kwargs = {
            "host": self.td_host,
            "user": self.td_user,
            "password": self.td_pass,
            "logmech": logmech,
            "SSLMode": "Allow",
            "dbs_port": 1025,
        }

    @contextmanager
    def _connection(self):
        conn = cursor = None
        try:
            conn = teradatasql.connect(**self._conn_kwargs)
            cursor = conn.cursor()
            yield cursor
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"Unable to connect to Teradata: {exc}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception as exc:
                    self.logger.error(f"Teradata cursor cleanup failed: {exc}")
            if conn:
                try:
                    conn.close()
                except Exception as exc:
                    self.logger.error(f"Teradata connection cleanup failed: {exc}")

    def _query(self, sql: str, headers: bool = False) -> list[tuple] | list[dict]:
        with self._connection() as cursor:
            cursor.execute(sql)
            rows: list[list] = cursor.fetchall()
            if not headers:
                return [tuple(r) for r in rows]

            col_names = [desc[0] for desc in cursor.description]
            return [dict(zip(col_names, row)) for row in rows]
