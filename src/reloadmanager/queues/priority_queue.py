import time
from pyspark.sql import DataFrame

from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.queues.models import QueueRecord
from reloadmanager.utils.event_time import EventTime
from reloadmanager.mixins.logging_mixin import LoggingMixin


class PriorityQueue(LoggingMixin):
    def __init__(self,
                 queue_schema: str,
                 catalog: str | None = None,
                 client: DatabricksRuntimeClient = None):
        self.schema: str = queue_schema
        self.client: DatabricksRuntimeClient = client or DatabricksRuntimeClient()
        self.catalog = catalog if catalog else ""
        self.catalog_schema: str = f"`{self.catalog}`." + self.schema
        self.queue_tbl: str = f"{self.catalog_schema}.QUEUE"
        self.queue_hist_tbl: str = f"{self.catalog_schema}.QUEUE_HISTORY"

    def create(self):

        self.client.query(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_schema}")

        self.client.query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.queue_tbl} (
                source_table STRING PRIMARY KEY,
                target_table STRING,
                where_clause STRING,
                primary_key STRING,
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
                    where_clause STRING,
                    primary_key STRING,
                    status STRING,
                    event_time STRING,
                    trigger_time STRING,
                    finish_time STRING,
                    duration_min FLOAT,
                    strategy STRING,
                    lock_rows BOOLEAN,
                    priority INTEGER,
                    error STRING,
                    num_records BIGINT,
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

        # Find the next eligible row to run, reference the priority VIEW defined by the user
        row: list[tuple] = self.client.query(f"""
        SELECT source_table, target_table, where_clause, primary_key, event_time, strategy, lock_rows, priority
        FROM {self.catalog_schema}.priorities_v
        WHERE rank = (
            SELECT MIN(rank)
            FROM {self.catalog_schema}.priorities_v
            WHERE strategy = '{strategy}'
            AND rank >= 1
        )
        AND strategy = '{strategy}'
        """)

        if not row:
            return ()

        source_table, target_table, where_clause, primary_key, event_time, strategy, lock_rows, priority = row[0]

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
                    source_table, target_table, where_clause, primary_key, status, event_time, trigger_time, strategy, lock_rows, priority
                ) VALUES (
                    '{source_table}', '{target_table}', "{where_clause}", '{primary_key}', 'RUNNING', '{event_time}', '{str(now)}',
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

        return source_table, target_table, where_clause, primary_key, lock_rows, event_time

    def upsert(self, tables: list[QueueRecord]) -> None:
        """
        Insert new rows into the Delta queue table or, when the row already exists
        and is still queued, refresh its priority + latest-event timestamp.
        """

        values_sql = ",\n  ".join(row.to_sql_values() for row in tables)

        sql: str = f"""
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
        """

        self.logger.debug(f"Upsert query:\n{sql}")

        self.client.query(sql)

    def batch_upsert_spark(self, tables_sdf: DataFrame) -> None:
        """
        Insert new rows into the Delta queue using a Spark DataFrame
        The Dataframe can have schema like QueueRecord:
            - source_table (required)
            - strategy (required)
            - target_table (defaults to {self.catalog}.source_table)
            - where_clause (default "")
            - primary_key (default "")
            - event_time (default EventTime.now())
            - lock_rows (default True)
            - priority (default 1)
        )
        """

        from delta.tables import DeltaTable
        from pyspark.sql.functions import col, when, lit, concat

        input_cols: list = tables_sdf.columns

        req_columns: set[str] = {'source_table', 'strategy'}
        if req_columns.difference(input_cols):
            raise ValueError(f"Both required columns: {str(req_columns)} not found in input dataframe, only: {input_cols}")

        all_columns: set[str] = \
            {'source_table', 'strategy', 'target_table', 'where_clause', 'primary_key', 'event_time', 'lock_rows', 'priority'}
        if not (forbidden := all_columns.difference(input_cols)):
            raise ValueError(f"Found forbidden column(s) {str(forbidden)}. Columns allowed: {all_columns}")

        # Inject missing columns with default values
        if 'target_table' not in input_cols:
            tables_sdf = tables_sdf.withColumn('target_table', lit(None).cast("string"))
        if 'where_clause' not in input_cols:
            tables_sdf = tables_sdf.withColumn('where_clause', lit(None).cast("string"))
        if 'primary_key' not in input_cols:
            tables_sdf = tables_sdf.withColumn('primary_key', lit(None).cast("string"))
        if 'event_time' not in input_cols:
            tables_sdf = tables_sdf.withColumn('event_time', lit(None).cast("string"))
        if 'lock_rows' not in input_cols:
            tables_sdf = tables_sdf.withColumn('lock_rows', lit(None).cast("boolean"))
        if 'priority' not in input_cols:
            tables_sdf = tables_sdf.withColumn('priority', lit(None).cast("int"))

        now_str: str = str(EventTime.now())
        tables_sdf: DataFrame = tables_sdf.select(
            col("source_table"),
            when(col("target_table").isNotNull(), col("target_table"))
            .otherwise(concat(lit(f"{self.catalog}."), col("source_table"))).alias("target_table"),
            when(col("where_clause").isNotNull(), col("where_clause")).otherwise(lit("")).alias("where_clause"),
            when(col("primary_key").isNotNull(), col("primary_key")).otherwise(lit("")).alias("primary_key"),
            when(col("event_time").isNotNull(), col("event_time")).otherwise(lit(now_str)).alias("event_time"),
            col("strategy"),
            when(col("lock_rows").isNotNull(), col("lock_rows")).otherwise(lit(True)).alias("lock_rows"),
            lit('Q').alias("status"),
            when(col("priority").isNotNull(), col("priority")).otherwise(lit(1)).alias("priority")
        )

        delta_table = DeltaTable.forName(self.client.spark, self.queue_tbl)

        delta_table.alias("t").merge(
            tables_sdf.alias("s"),
            "t.source_table = s.source_table"
        ).whenMatchedUpdate(set={
            "target_table": "s.target_table",
            "where_clause": "s.where_clause",
            "primary_key": "s.primary_key",
            "event_time": "s.event_time",
            "strategy": "s.strategy",
            "lock_rows": "s.lock_rows",
            "status": "s.status",
            "priority": "s.priority"
        }).whenNotMatchedInsert(values={
            "source_table": "s.source_table",
            "target_table": "s.target_table",
            "where_clause": "s.where_clause",   
            "primary_key": "s.primary_key",
            "event_time": "s.event_time",
            "strategy": "s.strategy",
            "lock_rows": "s.lock_rows",
            "status": "s.status",
            "priority": "s.priority"
        }).execute()

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
