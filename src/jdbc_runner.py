# Databricks notebook source
# MAGIC %md
# MAGIC # JDBC Runner
# MAGIC Run JDBC table loads

# COMMAND ----------

from dataclasses import dataclass, fields
import time
from databricks.sdk.runtime import dbutils
import subprocess

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "America/Phoenix")

# COMMAND ----------

dbutils.widgets.text("wheel_path", "")
wheel_path: str = dbutils.widgets.get("wheel_path")

# COMMAND ----------

wheel_path = wheel_path + "/.internal"
wheel_files = dbutils.fs.ls(wheel_path)
wheel_file_name = [file.name for file in wheel_files if file.name.endswith('.whl')][0]
full_wheel_file_path = wheel_path + "/" + wheel_file_name

# COMMAND ----------

subprocess.check_call([
    'pip',
    'install',
    full_wheel_file_path
])
subprocess.check_call([
    'pip',
    'install',
    'teradatasql'
])

# COMMAND ----------

from reloadmanager.mixins.logging_mixin import LoggingMixin
from reloadmanager.table_loader.report_record import ReportRecord
from reloadmanager.table_loader.table_reloader import TableReloader

# COMMAND ----------

# set parameters
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("where_clause", "")
dbutils.widgets.text("log_level", "")

source_table: str = dbutils.widgets.get("source_table")
target_table: str = dbutils.widgets.get("target_table")
where_clause: str = dbutils.widgets.get("where_clause")
log_level: str = dbutils.widgets.get("log_level")

logs = LoggingMixin()
logs.set_logger_level(log_level)

# COMMAND ----------

logs.logger.info("Starting reload...")

reloader: TableReloader = TableReloader(
    source_table=source_table,
    target_table=target_table,
    where_clause=where_clause,
    strategy="JDBC",
    lock_rows=True
)

metrics: ReportRecord = reloader.reload()

# COMMAND ----------

dbutils.notebook.exit(repr(metrics))
