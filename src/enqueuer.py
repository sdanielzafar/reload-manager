# Databricks notebook source
# MAGIC %md
# MAGIC # Enqueuer Job
# MAGIC The enqueuer job will query Teradata's tracking table and put tables into the queue to be processed

# COMMAND ----------

from dataclasses import dataclass, replace
import time

from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.clients.teradata_client import TeradataClient
from reloadmanager.priority_queue.models import QueueRecord, TableAttrRecord
from reloadmanager.priority_queue.priority_queue import PriorityQueue
from reloadmanager.utils.event_time import EventTime
from reloadmanager.mixins.logging_mixin import LoggingMixin

logs = LoggingMixin()
logs.set_logger_level("info")
EventTime.set_timezone("America/Phoenix")


# COMMAND ----------

# set parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("queue_schema", "reloadmanager")
dbutils.widgets.text("starting_watermark", "")
dbutils.widgets.text("reset_queue", "False")

catalog: str = dbutils.widgets.get("catalog")
queue_schema: str = dbutils.widgets.get("queue_schema")
starting_watermark: EventTime = EventTime.from_epoch(int(dbutils.widgets.get("starting_watermark")[:-3]))
reset_queue_str: str = dbutils.widgets.get("reset_queue")
reset_queue: bool = {"true": True, "false": False}[reset_queue_str.strip().lower()]


# COMMAND ----------

# set up things we need
demographic_table: str = "reloadmanager.tr_tables_writenos"
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

def set_priority(load_time: str, min_staleness: int, max_staleness: int) -> int:
    staleness: int = int(time.time()) - EventTime(load_time)
    if staleness < min_staleness:
        return 0
    else:
        match max_staleness - staleness:
            case t if t > 60:
                return 1
            case t if t > 45:
                return 2
            case t if t > 30:
                return 3
            case t if t > 15:
                return 4
            case t if t > 10:
                return 5
            case t if t > 5:
                return 6
            case t if t > 3:
                return 7
            case t if t > 2:
                return 8
            case t if t > 1:
                return 9
            case t if t > 0:
                return 10
            case t if t > -5:
                return 15
            case t if t > -10:
                return 20
            case _:
                return 25


# COMMAND ----------

def update_priority(queued_tables: list[QueueRecord], new_tables: list[TrackerRecord]) -> list[QueueRecord]:
    tables: set[str] = {r.source_table for r in new_tables} | {q.source_table for q in queued_tables}
    tbl_metadata: dict[str, TableAttrRecord] = get_table_metadata(tables)

    # augment the new tables with the metadata
    new_tables_queue: list[QueueRecord] = [
        QueueRecord(
            record.source_table,
            f"{catalog}.{(attrs := tbl_metadata[record.source_table]).target_table}",
            "",
            str(record.event_time),
            None,
            attrs.strategy,
            True,
            'Q',
            attrs.priority,
            None
        ) for record in new_tables
        # some tables in tracking table are actually CDC, so we only include if they are in the metadata table
        if record.source_table in tbl_metadata.keys()
    ]

    logs.logger.info(f"Found {len(new_tables_queue)} tables to enqueue.")
    logs.logger.debug(f"{str(new_tables_queue)}")

    # update priorities for all, sorry this is ugly
    return [
        replace(
            t,
            priority=set_priority(
                t.event_time,
                (attrs := tbl_metadata[t.source_table]).min_staleness,
                attrs.max_staleness
            ) * attrs.priority
        ) for t in queued_tables + new_tables_queue
    ]


# COMMAND ----------

logs.logger.info(f"Initializing..")
queue.create()
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
    new_tables: list[QueueRecord] = update_priority(queue_tables, updated_tables)

    if not new_tables:
        logs.logger.info(f"No tables in queue")
        time.sleep(60)
        continue

    # put them in the queue
    queue.upsert_queued(new_tables)
    logs.logger.info(f"Enqueud new tables and updated priorities.")

    # update watermark
    watermark = EventTime(queue.last_load_time())

    num_queued: int = len(new_tables)
    logs.logger.info(f"PROGRESS: {num_queued} tables in queue")