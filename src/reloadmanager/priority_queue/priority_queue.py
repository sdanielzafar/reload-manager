import time

from reloadmanager.queue.models import QueueRecord
from reloadmanager.utils.event_time import EventTime
from reloadmanager.clients.databricks_client_factory import get_dbx_client
from reloadmanager.clients.generic_database_client import GenericDatabaseClient
from reloadmanager.mixins.logging_mixin import LoggingMixin


class PriorityQueue(LoggingMixin):
    def __init__(self, queue_schema: str, catalog: str = str | None, client: GenericDatabaseClient = None):
        self.schema: str = queue_schema
        self.client: GenericDatabaseClient = client if client else get_dbx_client()
        self.catalog_schema: str = (f"`{catalog}`." if catalog else "") + self.schema
        self.queue_tbl: str = f"{self.catalog_schema}.QUEUE"
        self.queue_hist_tbl: str = f"{self.catalog_schema}.QUEUE_HISTORY"

    def create(self):

        self.client.query(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_schema}")

        self.client.query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.queue_tbl} (
                source_table STRING PRIMARY KEY,
                target_table STRING,
                event_time STRING,
                trigger_time STRING,
                strategy STRING,
                lock_rows BOOLEAN,
                status CHAR(1),
                priority INTEGER,
                event_time_latest STRING
            )
            """
        )

        self.client.query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.queue_hist_tbl} (
                    source_table STRING,
                    target_table STRING,
                    status STRING,
                    event_time STRING,
                    trigger_time STRING,
                    finish_time STRING,
                    duration_min FLOAT,
                    strategy STRING,
                    lock_rows BOOLEAN,
                    priority INTEGER,
                    error STRING,
                    num_records INTEGER,
                    PRIMARY KEY (source_table, event_time)
            )
            """
        )

    def truncate(self):
        self.client.query(f"TRUNCATE TABLE {self.queue_tbl}")
        self.client.query(f"TRUNCATE TABLE {self.queue_hist_tbl}")

    def requeue_running(self):

        # Get the rows to be requeued
        rows = self.client.query(f"""
            SELECT source_table, event_time
            FROM {self.queue_tbl}
            WHERE status = 'R'
        """)

        # Requeue them
        self.client.query(f"""
            UPDATE {self.queue_tbl}
            SET status = 'Q'
            WHERE status = 'R'
        """)

        if rows:
            values = ",".join(f"('{t}', '{et}')" for t, et in rows)
            self.client.query(f"""
                DELETE FROM {self.queue_hist_tbl}
                WHERE (source_table, event_time) IN ({values})
        """)

    def poll(self, strategy: str) -> tuple:
        now = EventTime.from_epoch(int(time.time()))

        # Find the next eligible row to run
        row: list[tuple] = self.client.query(f"""
        WITH ranked AS (
          SELECT *,
                 ROW_NUMBER() OVER (ORDER BY priority DESC) as rn
          FROM {self.queue_tbl}
          WHERE status = 'Q'
            AND strategy = '{strategy}'
            AND priority > 0
        )
        SELECT source_table, target_table, event_time, strategy, lock_rows, priority
        FROM ranked
        WHERE rn = 1
        """)

        if not row:
            return ()

        source_table, target_table, event_time, strategy, lock_rows, priority = row[0]

        # Mark it as running and update trigger_time
        self.client.query(f"""
            UPDATE {self.queue_tbl}
            SET status = 'R',
                trigger_time = '{str(now)}'
            WHERE source_table = '{source_table}'
        """)

        # Check if already exists in QUEUE_HISTORY
        existing = self.client.query(f"""
            SELECT status
            FROM {self.queue_hist_tbl}
            WHERE source_table = '{source_table}'
              AND event_time = '{event_time}'
        """)

        if not existing:
            # Insert as RUNNING if it doesn't exist
            self.client.query(f"""
                INSERT INTO {self.queue_hist_tbl} (
                    source_table, target_table, status, event_time, trigger_time, strategy, lock_rows, priority
                ) VALUES (
                    '{source_table}', '{target_table}', 'RUNNING', '{event_time}', '{str(now)}',
                    '{strategy}', {lock_rows}, {priority}
                )
            """)
        elif existing[0][0] in {"SUCCESS", "FAILED"}:
            self.logger.warning(
                f"Source table: {source_table} with event time {event_time} was previously processed "
                f"with status {existing[0]}, removing from QUEUE_HISTORY and reprocessing it."
            )
            self.client.query(f"""
                DELETE FROM {self.queue_hist_tbl}
                WHERE source_table = '{source_table}'
                  AND event_time = '{event_time}'
            """)

        return source_table, target_table, lock_rows, event_time

    def upsert_queued(self, tables: list[QueueRecord]):
        """
        Insert new rows into the Delta queue table or, when the row already exists
        and is still queued, refresh its priority + latest-event timestamp.
        """

        values_sql = ",\n  ".join(row.to_sql_values() for row in tables)

        self.client.query(f"""
            WITH src ({QueueRecord.fields_str()}) AS (
              VALUES {values_sql}
            )
            MERGE INTO {self.queue_tbl} AS tgt
            USING src
              ON tgt.source_table = src.source_table
            
            WHEN MATCHED AND tgt.status = 'Q' THEN
              UPDATE SET
                tgt.priority = src.priority,
                tgt.event_time_latest = src.event_time
            
            WHEN NOT MATCHED THEN
              INSERT *
        """)

    @property
    def in_queue(self) -> list[QueueRecord]:
        rows: list[tuple] = self.client.query(f"""
            SELECT * FROM {self.queue_tbl}
            WHERE status = 'Q'
        """)

        return [QueueRecord(*row) for row in rows]

    # call this when the job finishes
    def dequeue(
            self,
            source_table: str,
            event_time: str,
            end_time: float,
            duration: float,
            n_records: int,
            status: str,
            error: str,
    ) -> None:
        """
        Call this when the job finishes and we gotta remove the job from the queue
        """

        end_time_str = str(EventTime.from_epoch(int(end_time)))

        self.client.query(f"""
            DELETE FROM {self.queue_tbl}
            WHERE source_table = '{source_table}'
              AND event_time   = '{event_time}'
              AND status       = 'R'
        """)

        self.client.query(f"""
            UPDATE {self.queue_hist_tbl}
            SET  status       = '{status}',
                 finish_time  = '{end_time_str}',
                 duration_min = {duration},
                 num_records  = {n_records},
                 error        = '{error.replace("'", "")}'
            WHERE source_table = '{source_table}'
              AND event_time   = '{event_time}'
        """)

    def last_load_time(self) -> str:
        rows: list[tuple] = self.client.query(f"""
            SELECT MAX(et) AS max_et
            FROM (
                SELECT MAX(COALESCE(event_time_latest, event_time)) AS et FROM {self.queue_tbl}
                UNION ALL
                SELECT MAX(event_time) AS et FROM {self.queue_hist_tbl}
            )
        """)

        return str(rows[0][0]) if rows and rows[0][0] is not None else ""

    def __len__(self) -> int:
        try:
            rows: list[tuple] = self.client.query(
                f"SELECT COUNT(*) FROM {self.queue_tbl} WHERE status = 'Q'"
            )
            return rows[0][0] if rows else 0

        except Exception as e:
            if "not found" in str(e):
                return 0
            raise

