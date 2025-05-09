# Databricks notebook source
# MAGIC %md
# MAGIC # Dispatcher Job
# MAGIC The enqueuer job will spin up multiple threads to take data from the queue and process it

# COMMAND ----------

from datetime import datetime
import traceback
import time
from threading import Thread, Event, Lock
from databricks.sdk.runtime import dbutils

from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.priority_queue.priority_queue import PriorityQueue
from reloadmanager.table_loader.report_record import ReportRecord
from reloadmanager.utils.event_time import EventTime
from reloadmanager.mixins.logging_mixin import LoggingMixin
from reloadmanager.utils.synchronization import LogLock

logs = LoggingMixin()

# COMMAND ----------

# set parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("queue_schema", "reloadmanager")
dbutils.widgets.text("writenos_jobid", "")
dbutils.widgets.text("writenos_threads", "")
dbutils.widgets.text("jdbc_jobid", "0")
dbutils.widgets.text("jdbc_threads", "")
dbutils.widgets.text("source_tz", "")
dbutils.widgets.text("log_level", "")

catalog: str = dbutils.widgets.get("catalog")
queue_schema: str = dbutils.widgets.get("queue_schema")
writenos_jobid: int = int(dbutils.widgets.get("writenos_jobid"))
writenos_threads: int = int(dbutils.widgets.get("writenos_threads"))
jdbc_jobid: int = int(dbutils.widgets.get("jdbc_jobid"))
jdbc_threads: int = int(dbutils.widgets.get("jdbc_threads"))
source_tz: str = dbutils.widgets.get("source_tz")
log_level: str = dbutils.widgets.get("log_level")

logs.set_logger_level(log_level)

print(f"Source tz is: {source_tz}")
EventTime.set_timezone(source_tz)

dbx_client: DatabricksRuntimeClient = DatabricksRuntimeClient()
queue: PriorityQueue = PriorityQueue(queue_schema, catalog)
job_ids: dict[str, int] = {
    "WriteNOS": writenos_jobid,
    "JDBC": jdbc_jobid
}
stop_signal: Event = Event()
queue_lock: Lock = Lock()


# COMMAND ----------
class LoaderThread(Thread, LoggingMixin):
    def __init__(self,
                 thread_id: int,
                 strategy: str,
                 reload_job_id: int,
                 stop_signal: Event,
                 queue_lock: Lock,
                 priority_queue: PriorityQueue = queue,
                 databricks_client: DatabricksRuntimeClient = DatabricksRuntimeClient):
        super().__init__()
        self.strategy: str = strategy
        self.thread_id: str = f"{strategy.lower()}_{str(thread_id)}"
        self.logger.info(f"Thread {thread_id} starting...")
        self.stop_thread: bool = False
        self.stop_signal: Event = stop_signal
        self.queue_lock: Lock = queue_lock
        self.reload_job_id: int = reload_job_id
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

        with self.queue_lock:
            task: tuple = self.queue.poll(self.strategy)

        # if a WriteNOS thread does not find a WriteNOS table to load, it will take a JDBC one
        if not task and self.strategy == "WriteNOS":
            with self.queue_lock:
                task: tuple = self.queue.poll("JDBC")

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
            with self.queue_lock:
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
        params: dict[str, str] = {
            "source_table": source_table,
            "target_table": target_table,
            "where_clause": where_clause,
            "lock_rows": lock_rows
        }
        output = dbx_client.trigger_job(self.reload_job_id, params, get_output=True)
        return eval(output)


# COMMAND ----------

# start worker threads
def create_workers(strategy, count, reload_job_id) -> list[LoaderThread]:
    if count > 0:
        logs.logger.info(f"Creating {count} {strategy} threads...")
        threads = [
            LoaderThread(i, strategy, reload_job_id, stop_signal, queue_lock)
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
    "JDBC": create_workers("JDBC", jdbc_threads, job_ids["JDBC"]),
    "WriteNOS": create_workers("WriteNOS", writenos_threads, job_ids["WriteNOS"])
}

# COMMAND ----------

# let the session hang until all threads complete
for thread in thread_pool["JDBC"] + thread_pool["WriteNOS"]:
    thread.join()
