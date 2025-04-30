import logging
from databricks.sdk.runtime import dbutils

from reloadmanager.table_loader.table_reloader import TableReloader


def main(args):
    logger = logging.getLogger("reloadmanager")
    logger.setLevel(logging.DEBUG)

    # Attach a handler with custom formatting
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    logger.info("Starting reload...")

    reloader: TableReloader = TableReloader(
        source_table=args.source_table,
        target_table=args.target_table,
        where_clause=args.where_clause,
        strategy=args.strategy,
        lock_rows=args.lock_rows
    )

    dbutils.jobs.taskValues.set(
        key="result",
        value=repr(reloader.reload())
    )
