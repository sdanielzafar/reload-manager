import argparse
from reloadmanager.cli import reload_table, query_teradata, query_databricks


def main():
    parser = argparse.ArgumentParser(
        description="NXP Reload Manager CLI"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: reload_table
    reload_parser = subparsers.add_parser("reload-table", help="Reload a single table")
    reload_parser.add_argument("--source-table", required=True, help="Teradata: schema.table")
    reload_parser.add_argument("--target-table", required=True, help="Databricks: catalog.schema.table")
    reload_parser.add_argument("--where-clause", required=False, help="a SQL WHERE clause to limit data", default=None)
    reload_parser.add_argument("--strategy", required=False, help="'WriteNOS', 'JDBC', or 'TPT", default="WriteNOS")
    reload_parser.add_argument("--lock-rows", required=False, help="Whether to enable row locking", default=True)
    reload_parser.set_defaults(func=reload_table.main)

    # Subcommand: query_teradata
    td_query_parser = subparsers.add_parser("query-teradata", help="Query Teradata")
    td_query_parser.add_argument("--query", required=True, help="Query")
    td_query_parser.add_argument("--headers", required=False, help="Whether to display headers", default=False)
    td_query_parser.set_defaults(func=query_teradata.main)

    # Subcommand: query_databricks
    dbx_query_parser = subparsers.add_parser("query-databricks", help="Query Databricks")
    dbx_query_parser.add_argument("--query", required=True, help="Query")
    dbx_query_parser.add_argument("--headers", required=False, help="Whether to display headers", default=False)
    dbx_query_parser.set_defaults(func=query_databricks.main)

    args = parser.parse_args()
    args.func(args)
