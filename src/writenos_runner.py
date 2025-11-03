# Databricks notebook source
# MAGIC %md
# MAGIC # WriteNOS Runner
# MAGIC Run WriteNOS table loads

# COMMAND ----------

from databricks.sdk.runtime import dbutils

from reloadmanager.mixins.logging_mixin import LoggingMixin
from reloadmanager.table_loader.report_record import ReportRecord
from reloadmanager.table_loader.table_reloader import TableReloader

# COMMAND ----------

# set parameters
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("where_clause", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("log_level", "")
# dbutils.widgets.text("create_table_if_not_exists", "")

source_table: str = dbutils.widgets.get("source_table")
target_table: str = dbutils.widgets.get("target_table")
where_clause: str = dbutils.widgets.get("where_clause")
primary_key: str = dbutils.widgets.get("primary_key")
log_level: str = dbutils.widgets.get("log_level")
# create_table_if_not_exists: bool = (dbutils.widgets.get("create_table_if_not_exists").strip().lower() == 'true')

logs = LoggingMixin()
logs.set_logger_level(log_level)

# COMMAND ----------

logs.logger.info("Starting reload...")

reloader: TableReloader = TableReloader(
    source_table=source_table,
    target_table=target_table,
    where_clause=where_clause,
    primary_key=primary_key,
    strategy="WriteNOS",
    lock_rows=True
)

metrics: ReportRecord = reloader.reload()

# COMMAND ----------

dbutils.notebook.exit(repr(metrics))
