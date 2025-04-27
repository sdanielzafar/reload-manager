from reloadmanager.clients.generic_database_client import GenericDatabaseClient


class DatabricksRuntimeClient(GenericDatabaseClient):
    """Runs *inside* a Databricks notebook / cluster / SQL-warehouse."""

    def __init__(self):
        super().__init__()
        from databricks.sdk import WorkspaceClient  # only available on-platform
        self.ws = WorkspaceClient()

    def _query(self, sql: str, headers: bool = False) -> list:
        df = spark.sql(sql)
        rows = df.collect()
        return [r.asDict() if headers else tuple([*r]) for r in rows]

    # — extra helper: trigger job —
    def trigger_job(self, job_id: int, params: dict[str, str] | None = None) -> int:
        run = self.ws.jobs.run_now(job_id=job_id, notebook_params=params or {})
        return run.run_id
