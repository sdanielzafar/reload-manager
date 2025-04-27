from reloadmanager.clients.databricks_client_factory import get_dbx_client
from reloadmanager.clients.generic_database_client import GenericDatabaseClient


def main(args):
    dbx_client: GenericDatabaseClient = get_dbx_client()
    results = dbx_client.query(args.query, args.headers)
    for row in results:
        print(row)
