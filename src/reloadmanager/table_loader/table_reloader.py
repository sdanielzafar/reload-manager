import time
from functools import cached_property

from reloadmanager.generics.generic_runner import GenericRunner, RunnerError
from reloadmanager.jdbc.jdbc_config_builder import JDBCConfigBuilder
from reloadmanager.jdbc.jdbc_runner import JDBCRunner
from reloadmanager.writenos.writenos_runner import WriteNOSRunner
from reloadmanager.writenos.writenos_config_builder import WriteNOSConfigBuilder
from reloadmanager.mixins.logging_mixin import LoggingMixin
from reloadmanager.table_loader.report_record import ReportRecord


class TableReloader(LoggingMixin):
    def __init__(self,
                 source_table: str,
                 target_table: str,
                 where_clause: str,
                 primary_key: str,
                 strategy: str,
                 lock_rows: bool):
        self.source_table: str = source_table
        self.target_table: str = target_table
        self.where_clause: str = where_clause
        self.primary_key: str = primary_key
        self.strategy: str = strategy.lower()
        self.lock_rows: bool = lock_rows
        # self.create_table_if_not_exists: bool = create_table_if_not_exists
    @cached_property
    def builder(self) -> WriteNOSConfigBuilder | JDBCConfigBuilder:
        match self.strategy:
            case "writenos":
                return WriteNOSConfigBuilder(
                    source_table=self.source_table,
                    target_table=self.target_table,
                    where_clause=self.where_clause,
                    primary_key=self.primary_key,
                    lock_rows=self.lock_rows
                    # create_table_if_not_exists=self.create_table_if_not_exists
                )
            case "jdbc":
                return JDBCConfigBuilder(
                    source_table=self.source_table,
                    target_table=self.target_table,
                    where_clause=self.where_clause,
                    primary_key=self.primary_key,
                    lock_rows=self.lock_rows
                    # create_table_if_not_exists=self.create_table_if_not_exists
                )
            case other:
                raise NotImplementedError(f"Strategy: '{other}' has not been implemented")

    @cached_property
    def runner(self) -> GenericRunner:
        match self.strategy:
            case "writenos":
                return WriteNOSRunner(builder=self.builder)
            case "jdbc":
                return JDBCRunner(builder=self.builder)
            case other:
                raise NotImplemented(f"Strategy: '{other}' has not been implemented")

    def reload(self) -> ReportRecord:
        status = "SUCCESS"
        error = ""
        num_records: int = 0
        start: float = time.time()
        try:
            self.runner.run_snapshot()
            num_records = self.runner.num_records
        except RunnerError as e:
            status = "FAILED"
            self.logger.error(str(e))
            error = str(e) or ""
        finally:
            end: float = time.time()

        report_record = ReportRecord(self.source_table, self.strategy, status, start, end, num_records, error)
        self.logger.info(f"{status}: duration {report_record.duration:.2f} minutes")
        return report_record
