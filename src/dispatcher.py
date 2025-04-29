# Databricks notebook source
# MAGIC %md
# MAGIC # Dispatcher Job
# MAGIC The enqueuer job will spin up multiple threads to take data from the queue and process it

# COMMAND ----------

from datetime import datetime
import traceback
import time
from threading import Thread, Event

from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.priority_queue.priority_queue import PriorityQueue
from reloadmanager.table_loader.report_record import ReportRecord
from reloadmanager.utils.event_time import EventTime
from reloadmanager.mixins.logging_mixin import LoggingMixin
from reloadmanager.utils.synchronization import LogLock

logs = LoggingMixin()
logs.set_logger_level("debug")

# COMMAND ----------

# set parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("queue_schema", "reloadmanager")
dbutils.widgets.text("writenos_threads", "")
dbutils.widgets.text("jdbc_threads", "")
dbutils.widgets.text("source_tz", "")

catalog: str = dbutils.widgets.get("catalog")
queue_schema: str = dbutils.widgets.get("queue_schema")
writenos_threads: int = int(dbutils.widgets.get("writenos_threads"))
jdbc_threads: int = int(dbutils.widgets.get("jdbc_threads"))
source_tz: str = dbutils.widgets.get("source_tz")

print(f"Source tz is: {source_tz}")
EventTime.set_timezone(source_tz)

dbx_client: DatabricksRuntimeClient = DatabricksRuntimeClient()
queue: PriorityQueue = PriorityQueue(queue_schema, catalog)
stop_signal: Event = Event()


# COMMAND ----------
class LoaderThread(Thread, LoggingMixin):
    def __init__(self,
                 thread_id: int,
                 strategy: str,
                 stop_signal: Event,
                 priority_queue: PriorityQueue = queue,
                 databricks_client: DatabricksRuntimeClient = DatabricksRuntimeClient):
        super().__init__()
        self.strategy: str = strategy
        self.thread_id: str = f"{strategy.lower()}_{str(thread_id)}"
        self.logger.info(f"Thread {thread_id} starting...")
        self.stop_thread: bool = False
        self.stop_signal = stop_signal
        self.queue: PriorityQueue = priority_queue
        self.databricks_client: DatabricksRuntimeClient = databricks_client

    def wait_if_needed(self):
        messaged = False
        while True:
            current_minute = datetime.now().minute
            if current_minute > 2:
                break
            if not messaged:
                self.logger.info(f"Thread {self.thread_id} is waiting till minute 3 to proceed. Sleeping...")
                messaged = True
            time.sleep(2)

    def task(self):

        self.wait_if_needed()

        task: tuple = self.queue.poll(self.strategy)

        if not task:
            self.logger.info(f"Thread {self.thread_id} found no queued tables. Sleeping...")
            time.sleep(60)
            return None

        source_table, target_table, where_clause, lock_rows, event_time = task
        duration, end_time, n_records, status, error = 0.0, 0.0, 0, 'Failed', ""
        try:
            self.logger.info(f"Thread {self.thread_id} picked up {source_table}...")
            # reload the table
            result: ReportRecord = self.reload_table(source_table, target_table, where_clause, lock_rows)

            self.logger.info(f"Thread {self.thread_id} reloaded table '{source_table}'")
            duration, end_time, n_records, status, error = \
                result.duration, result.end, result.num_records, result.status, result.error
        except Exception as e:
            self.logger.error(f"CRITICAL FAILURE: Thread {self.thread_id} failed to reload '{source_table}': {e}")
            traceback.print_exc()
        finally:
            # remove table from queue
            self.queue.dequeue(source_table, event_time, end_time, duration, n_records, status, error)

    def run(self):
        try:
            time.sleep(10)
            while not self.stop_signal.is_set():
                self.task()
                if self.stop_thread:
                    break
            if self.stop_signal.is_set():
                self.logger.info(f"Thread {self.thread_id} received stop signal. Exiting.")
        except Exception:
            with LogLock.lock:
                traceback.print_exc()

    def reload_table(self, source_table: str, target_table: str, where_clause: str, lock_rows: bool) -> ReportRecord:
        time.sleep(15)
        self.logger.info(f"It was super hard and it took a long time, but {self.thread_id} finished {source_table}")
        return ReportRecord(source_table, self.strategy, "SUCCESS", 0.0, 0.9, 999, "")


# COMMAND ----------

# start worker threads
def create_workers(strategy, count) -> list[LoaderThread]:
    if count > 0:
        logs.logger.info(f"Creating {count} {strategy} threads...")
        threads = [
            LoaderThread(i, strategy, stop_signal)
            for i in range(count)
        ]
        for thread in threads:
            thread.start()
        return threads
    return []


def num_active_threads(_thead_pool: dict[str, list[LoaderThread]]) -> int:
    threads: list[LoaderThread] = _thead_pool["JDBC"] + _thead_pool["WriteNOS"]
    return len([t for t in threads if t.is_alive()])


thread_pool: dict[str, list[LoaderThread]] = {
    "JDBC": create_workers("JDBC", jdbc_threads),
    "WriteNOS": create_workers("WriteNOS", writenos_threads)
}
