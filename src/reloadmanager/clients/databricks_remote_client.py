import http.client
import json
import time
from contextlib import contextmanager

from reloadmanager.clients.generic_database_client import GenericDatabaseClient
from reloadmanager.mixins.secret_mixin import SecretMixin


class DatabricksRemoteClient(GenericDatabaseClient, SecretMixin):
    def __init__(self, secret_path: str | None = None):
        super().__init__()
        self.load_env_file(secret_path or "/home/arcion/secrets/.env")
        self.dbx_pat: str = self.get_secret("DBX_PAT")
        self.dbx_host = self.get_secret("DBX_HOST")
        self.dbx_warehouse = self.get_secret("DBX_WAREHOUSE")

    @contextmanager
    def _connection(self):
        conn = None
        try:
            conn = http.client.HTTPSConnection(self.dbx_host)
            yield conn
        finally:
            if conn:
                conn.close()

    def _query(self, sql: str, headers: bool = False) -> list[tuple] | list[dict]:
        with self._connection() as conn:
            # Submit query
            req_headers = {
                'Authorization': f'Bearer {self.dbx_pat}',
                'Content-Type': 'application/json'
            }
            body = {
                "statement": sql,
                "warehouse_id": self.dbx_warehouse
            }

            try:
                conn.request("POST", "/api/2.0/sql/statements", body=json.dumps(body), headers=req_headers)
                resp = conn.getresponse()
                data = json.loads(resp.read())
                statement_id = data['statement_id']
            except Exception as e:
                self.logger.error(f"Failed to submit query to Databricks Warehouse: {e}")
                raise

            # Poll for results
            while True:
                try:
                    conn.request("GET", f"/api/2.0/sql/statements/{statement_id}", headers=req_headers)
                    poll_resp = conn.getresponse()
                    result = json.loads(poll_resp.read())
                    state = result['status']['state']
                except Exception as e:
                    self.logger.error(f"Failed polling query status: {e}")
                    raise

                if state in ["SUCCEEDED", "FAILED", "CANCELED"]:
                    break
                time.sleep(1)

            if state != "SUCCEEDED":
                self.logger.error(f"Query failed with state: {state}")
                raise RuntimeError(f"Query failed: {result}")

            columns = result['manifest']['schema'].get("columns")
            if not columns:
                return []

            columns = [field['name'] for field in result['manifest']['schema']['columns']]
            rows = result['result'].get('data_array')
            if not rows:
                return []
            if headers:
                return [dict(zip(columns, row)) for row in rows]
            else:
                return [row for row in rows]

    def trigger_job(self, job_id: int, parameters: dict[str, str] = None) -> str:
        """Triggers a Databricks job with optional parameters. Returns the run_id."""
        parameters = parameters or {}
        headers = {
            'Authorization': f'Bearer {self.dbx_pat}',
            'Content-Type': 'application/json'
        }

        body = {
            "job_id": job_id,
            "notebook_params": parameters
        }

        with self._connection() as conn:
            try:
                conn.request("POST", "/api/2.1/jobs/run-now", body=json.dumps(body), headers=headers)
                resp = conn.getresponse()
                data = json.loads(resp.read())

                if resp.status != 200:
                    self.logger.error(f"Job trigger failed: {data}")
                    raise RuntimeError(f"Failed to trigger job: {data}")

                run_id = data.get("run_id")
                return run_id

            except Exception as e:
                self.logger.error(f"Exception while triggering job_id {job_id}: {e}")
                raise
