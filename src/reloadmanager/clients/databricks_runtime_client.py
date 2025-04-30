from functools import cached_property
from datetime import timedelta
from databricks.connect import DatabricksSession
from databricks.connect.session import SparkSession
from delta.connect.exceptions import ConcurrentAppendException, ConcurrentWriteException

from reloadmanager.clients.generic_database_client import GenericDatabaseClient


class DatabricksRuntimeClient(GenericDatabaseClient):
    """Runs *inside* a Databricks notebook / cluster / SQL-warehouse."""

    def __init__(self):
        super().__init__()
        from databricks.sdk import WorkspaceClient
        self.ws = WorkspaceClient()

    @cached_property
    def spark(self) -> SparkSession:
        try:
            return DatabricksSession.builder.getOrCreate()
        except ImportError:
            return SparkSession.builder.getOrCreate()

    def _query(self, sql: str, headers: bool = False) -> list:
        max_delta_concurrency_attempts: int = 5
        attempt = 0
        while attempt < max_delta_concurrency_attempts:
            try:
                df = self.spark.sql(sql)
                rows = df.collect()
                return [r.asDict() if headers else tuple([*r]) for r in rows]
            except (ConcurrentAppendException, ConcurrentWriteException):
                attempt += 1
            except Exception:
                raise

    def trigger_job(self, job_id: int, params: dict[str, str] | None = None, get_output=False):
        run = self.ws.jobs.run_now(job_id=job_id, notebook_params=params or {})
        if not get_output:
            return run.run_id
        run_id = run.result(timeout=timedelta(minutes=60))
        return self.ws.jobs.get_run_output(run_id.tasks[0].run_id).notebook_output.result
