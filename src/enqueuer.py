# Databricks notebook source
# MAGIC %md
# MAGIC # Enqueuer Job
# MAGIC The enqueuer job will query Teradata's tracking table and put tables into the queue to be processed

# COMMAND ----------

from dataclasses import dataclass, fields
import time
from databricks.sdk.runtime import dbutils

from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.clients.teradata_client import TeradataClient
from reloadmanager.queues.models import QueueRecord
from reloadmanager.queues.priority_queue import PriorityQueue
from reloadmanager.utils.event_time import EventTime
from reloadmanager.mixins.logging_mixin import LoggingMixin

logs = LoggingMixin()
EventTime.set_timezone("America/Phoenix")

# COMMAND ----------

# set parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("queue_schema", "reloadmanager")
dbutils.widgets.text("demographic_table", "reloadmanager.tr_tables_writenos")
dbutils.widgets.text("starting_watermark", "")
dbutils.widgets.text("reset_queue", "False")
dbutils.widgets.text("log_level", "info")

catalog: str = dbutils.widgets.get("catalog")
queue_schema: str = dbutils.widgets.get("queue_schema")
demographic_table: str = dbutils.widgets.get("demographic_table")
starting_watermark: EventTime = EventTime.from_epoch(int(dbutils.widgets.get("starting_watermark")[:-3]))
reset_queue_str: str = dbutils.widgets.get("reset_queue")
reset_queue: bool = {"true": True, "false": False}[reset_queue_str.strip().lower()]
log_level: str = dbutils.widgets.get("log_level")

# COMMAND ----------

# set up things we need
logs.set_logger_level(log_level)
td_client: TeradataClient = TeradataClient()
dbx_client: DatabricksRuntimeClient = DatabricksRuntimeClient()
queue: PriorityQueue = PriorityQueue(queue_schema, catalog)


# COMMAND ----------

def init_watermark(_watermark: EventTime) -> EventTime:
    """
    If there are things in the queue or queue history then the starting watermark is the latest timestamp from those
    If these are empty then we use the starting timestamp.
    We assume that all running jobs have been re-queued before this is called.
    """

    last_load_time: str = queue.last_load_time()
    if last_load_time:
        logs.logger.info(f"Determined watermark from the queue: {last_load_time}.")
        return EventTime(last_load_time)
    else:
        logs.logger.info(f"Using starting watermark: {str(_watermark)}.")
        return _watermark


# COMMAND ----------

def define_priority_view(p_queue: PriorityQueue) -> None:
    """
    Define how priorities are calculated, it just needs to expose a rank column. The consumer query looks like this:
        SELECT source_table, target_table, where_clause, primary_key, event_time, strategy, lock_rows, priority
        FROM {self.catalog_schema}.priorities_v
        WHERE rank = 1
        AND strategy = '{strategy}'

    This is defined in Enqueuer so that all the customer-specific business logic is decoupled from the queue and
    dispatcher
    """

    sql: str = f"""
    CREATE OR REPLACE VIEW {catalog}.{queue_schema}.priorities_v AS (
        SELECT source_table, target_table, where_clause, primary_key, event_time, strategy, lock_rows, priority,
        ROW_NUMBER() OVER (ORDER BY priority DESC, event_time ASC) as rank
        FROM (
            SELECT q.source_table, q.target_table, q.where_clause, q.primary_key, q.event_time, q.strategy, q.lock_rows, q.status,
            d.min_staleness, d.max_staleness,
            floor(
                (unix_timestamp(current_timestamp()) - 
                unix_timestamp(to_utc_timestamp(q.event_time, 'America/Phoenix'))) 
                / 60) AS staleness_m,
            (CASE WHEN staleness_m < d.min_staleness THEN 0                 
                  WHEN (max_staleness - staleness_m) > 60 THEN 1              
                  WHEN (max_staleness - staleness_m) > 45 THEN 2             
                  WHEN (max_staleness - staleness_m) > 30 THEN 3             
                  WHEN (max_staleness - staleness_m) > 15 THEN 4             
                  WHEN (max_staleness - staleness_m) > 10 THEN 5             
                  WHEN (max_staleness - staleness_m) > 5 THEN 6             
                  WHEN (max_staleness - staleness_m) > 3 THEN 7             
                  WHEN (max_staleness - staleness_m) > 2 THEN 8             
                  WHEN (max_staleness - staleness_m) > 1 THEN 9             
                  WHEN (max_staleness - staleness_m) > 0 THEN 10            
                  WHEN (max_staleness - staleness_m) > -5 THEN 15             
                  WHEN (max_staleness - staleness_m) > -10 THEN 20 
                  ELSE 25                
            END) * q.priority as priority
            FROM {p_queue.queue_tbl} q
            JOIN {demographic_table} d 
            ON q.source_table = d.source_table
        ) sub
        WHERE status = 'Q'
        AND priority > 0
    )
    """

    dbx_client.query(sql)


# COMMAND ----------

@dataclass(frozen=True)
class TrackerRecord:
    source_table: str
    event_time: EventTime


def query_tracking_table(watermark: EventTime, td_client: TeradataClient = td_client) -> list[TrackerRecord]:
    td_query: str = f"""
        SELECT 
            ObjectDatabaseName || '.' || ObjectTableName as tbl, 
            LoadCompletionTS as reload_ts 
        FROM EDWPC_SYNC.EBI_LOAD_COMPLETION_GOLD 
        WHERE LoadCompletionTS > '{watermark}'
        AND LoadCompletionTS <= '{EventTime.now()}' 
    """
    logs.logger.debug(f"Teradata query: {td_query}")
    rows: list[tuple] = td_client.query(td_query, max_attempts=200)
    return [TrackerRecord(tbl, EventTime.from_datetime_local(ts)) for tbl, ts in rows]


# COMMAND ----------

@dataclass(frozen=True)
class TableAttrRecord:
    source_table: str
    target_table: str
    ingestion_strategy: str
    cdc_watermark: str
    primary_key: str
    strategy: str
    disabled: bool
    priority: int
    min_staleness: int
    max_staleness: int

    @classmethod
    def from_tuple(cls, line: tuple):
        if len(line) != len(fields(cls)):
            raise ValueError(f"Input line {line} should have {len(fields(cls))} fields")

        source_table, target_table, ingestion_strategy, cdc_watermark, primary_key, strategy, disabled, priority, min_staleness, max_staleness = line

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
            ingestion_strategy,
            cdc_watermark,
            primary_key,
            strategy,
            disabled,
            int(priority),
            int(min_staleness or 0),
            int(max_staleness)
        )


def get_table_metadata(tables: set[str]) -> dict[str, TableAttrRecord]:
    if not tables:
        return set()
    tbl_vals: str = "','".join(tables)
    table_info: list[tuple] = dbx_client.query(
        f"SELECT * FROM {demographic_table} "
        f"WHERE source_table IN ('{tbl_vals}')"
    )
    return {(r := TableAttrRecord.from_tuple(line)).source_table: r for line in table_info}


# COMMAND ----------

def add_metadata(new_tables: list[TrackerRecord]) -> list[QueueRecord]:
    tables: set[str] = {r.source_table for r in new_tables}
    tbl_metadata: dict[str, TableAttrRecord] = get_table_metadata(tables)

    new_tables_queue: list[QueueRecord] = []

    for record in new_tables:
        if record.source_table not in tbl_metadata.keys():
            continue

        source_table: str = record.source_table
        attrs: TableAttrRecord = tbl_metadata[record.source_table]
        target_table: str = f"{catalog}.{attrs.target_table}"
        cdc_watermark: str | None = getattr(attrs, "cdc_watermark", None)
        primary_key: str | None = getattr(attrs, "primary_key", None)
        strategy: str = attrs.strategy
        where_clause: str = ""
        try:
            if cdc_watermark:
                max_val_result: list[tuple] = dbx_client.query(
                    f"SELECT CAST(MAX({cdc_watermark}) as string) as max_val FROM {target_table}"
                )
                max_val: str | None = max_val_result[0][0] if max_val_result and max_val_result[0][0] is not None else None

                if max_val is not None:
                    where_clause = f"{cdc_watermark} > \'{max_val}\'"
                    row_count_result: list[tuple] = td_client.query(
                        f"SELECT COUNT(1) as row_count FROM {source_table} WHERE {cdc_watermark} > '{max_val}'"
                    )
                    row_count: int = int(row_count_result[0][0]) if row_count_result else 0

                    strategy = "JDBC" if row_count < 200000 else "WriteNOS"
        except Exception as e:
            logs.logger.warning(f"Could not determine cdc watermarking strategy for {source_table}: {e}")
        # augment the new tables with the metadata
        new_tables_queue.append(
            QueueRecord(
                source_table,
                target_table,
                where_clause,
                primary_key,
                str(record.event_time),
                None,
                strategy,
                True,
                'Q',
                attrs.priority,
                None
            )
        )

    logs.logger.info(f"Found {len(new_tables_queue)} tables to enqueue.")
    logs.logger.debug(f"{str(new_tables_queue)}")

    return new_tables_queue


# COMMAND ----------

logs.logger.info(f"Initializing..")
queue.create()
define_priority_view(queue)
if reset_queue:
    logs.logger.info("Clearing the queue...")
    queue.truncate()
else:
    queue.requeue_running()

watermark: EventTime = init_watermark(starting_watermark)

# COMMAND ----------

while True:
    # grab tables from the Teradata tracking table
    logs.logger.info(f"Querying tracking table with watermark: {str(watermark)}.")
    updated_tables: list[TrackerRecord] = query_tracking_table(watermark)

    # grab tables from queue
    queue_tables: list[QueueRecord] = queue.in_queue

    # grab the table details from the flat file and update priority
    new_tables = add_metadata(updated_tables)

    if not new_tables:
        logs.logger.info(f"No tables")
        time.sleep(60)
        continue

    # put them in the queue
    if new_tables:
        queue.upsert(new_tables)
        logs.logger.info(f"Enqueued new tables")

    # update watermark
    watermark = EventTime(queue.last_load_time())

    num_queued: int = len(new_tables)
    logs.logger.info(f"PROGRESS: {num_queued} tables in queue")
