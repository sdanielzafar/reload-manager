from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.clients.databricks_remote_client import DatabricksRemoteClient
from reloadmanager.clients.generic_database_client import GenericDatabaseClient
import os


def get_dbx_client() -> GenericDatabaseClient:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return DatabricksRuntimeClient()

    return DatabricksRemoteClient()
