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
dbutils.widgets.text("starting_watermark", "")
dbutils.widgets.text("reset_queue", "False")
dbutils.widgets.text("log_level", "info")
dbutils.widgets.text("tracking_table", "")

catalog: str = dbutils.widgets.get("catalog")
queue_schema: str = dbutils.widgets.get("queue_schema")
starting_watermark: EventTime = EventTime.from_epoch(int(dbutils.widgets.get("starting_watermark")[:-3]))
reset_queue_str: str = dbutils.widgets.get("reset_queue")
reset_queue: bool = {"true": True, "false": False}[reset_queue_str.strip().lower()]
log_level: str = dbutils.widgets.get("log_level")
tracking_table: str = dbutils.widgets.get("tracking_table")

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
        SELECT source_table, target_table, where_clause, primary_key, event_time, strategy, lock_rows, 1 as priority,
        ROW_NUMBER() OVER (ORDER BY event_time ASC) as rank
        FROM {p_queue.queue_tbl}
        WHERE status = 'Q'
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
        FROM {tracking_table}
        WHERE LoadCompletionTS > '{watermark}'
        AND LoadCompletionTS <= '{EventTime.now()}' 
    """
    logs.logger.debug(f"Teradata query: {td_query}")
    rows: list[tuple] = td_client.query(td_query, max_attempts=200)
    return [TrackerRecord(tbl, EventTime.from_datetime_local(ts)) for tbl, ts in rows]


# COMMAND ----------

def add_metadata(new_tables: list[TrackerRecord]) -> list[QueueRecord]:

    new_tables_queue: list[QueueRecord] = []

    for record in new_tables:

        source_table: str = record.source_table
        primary_key: str | None = None
        priority: int = 1
        target_table: str = f"{catalog}.{source_table}"
        strategy: str = "WriteNOS"
        where_clause = ""

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
                priority,
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
    new_tables: list[QueueRecord] = add_metadata(updated_tables)

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
