from abc import ABC, abstractmethod
from dataclasses import astuple
from functools import cached_property

from reloadmanager.generics.generic_config_builder import GenericConfigBuilder
from reloadmanager.clients.databricks_runtime_client import DatabricksRuntimeClient
from reloadmanager.clients.teradata_interface import TeradataInterface
from reloadmanager.mixins.logging_mixin import LoggingMixin


class GenericRunner(ABC, LoggingMixin):
    builder: GenericConfigBuilder

    def __init__(self, builder: GenericConfigBuilder, source_interface=None, target_interface=None):
        self.builder: GenericConfigBuilder = builder
        self.source_interface: TeradataInterface = source_interface if source_interface \
            else TeradataInterface(str(self.builder.source_table), self.builder.lock_rows)
        self.target_interface: DatabricksRuntimeClient = target_interface if target_interface \
            else DatabricksRuntimeClient()
        self.num_records: int = 0

    @property
    def type_map(self):
        return {
            "int": "INTEGER",
            "tinyint": "TINYINT",
            "smallint": "SMALLINT",
            "bigint": "BIGINT",
            "varchar": "VARCHAR",
            "double": "DOUBLE PRECISION",
            "float": "FLOAT",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "timestamp": "TIMESTAMP",
            'binary': "BINARY"
        }

    @cached_property
    def target_schema(self) -> list[dict[str, str]]:
        tbl_info: list[dict[str, str]] = self.target_interface.query(
            f"DESCRIBE TABLE {str(self.builder.target_table)}",
            headers=True
        )
        # get rid of REPLICATE_IO_METADATA_VERSION
        tbl_info_cln: list[dict[str, str]] = [
            field for field in tbl_info
            if field["col_name"] != "REPLICATE_IO_METADATA_VERSION"
        ]

        # get rid of clustering info
        for i, field in enumerate(tbl_info_cln):
            if field["col_name"].startswith("#"):
                return tbl_info_cln[:i]

        return tbl_info_cln

    def get_column_type(self, cols: list[dict]) -> str:
        """
        Generates SQL select query and a dictionary of column names with their respective lengths/types.
        :param cols: the source table's columns as a list of dict, representing column name: value
        :return:
        """

        # Initialize the select query string
        select_query = "SELECT "

        # Iterate over each row in the DataFrame containing column metadata
        for row in cols:
            col_type = row['ColumnType'].strip()

            # Determine how to handle different column types
            if col_type in ('TS', 'SZ', 'MI', 'DH', 'DM', 'DS', 'DY', 'HM', 'HS', 'AT', 'TZ'):
                select_query = select_query + f"CAST (\"{row['ColumnName']}\" AS VARCHAR({row['ColumnLength']})) AS \"{row['ColumnName']}\" ,"
            elif col_type in ('DA',):
                select_query = select_query + f"CAST (CAST (\"{row['ColumnName']}\" AS DATE format 'YYYY-MM-DD') AS VARCHAR(10))  AS \"{row['ColumnName']}\" ,"
            elif col_type in ('CF',):
                col_len = row['ColumnFormat'].replace('X(', '').replace(')', '')
                select_query = select_query + f"CAST (\"{row['ColumnName']}\" AS VARCHAR({col_len})) AS \"{row['ColumnName']}\" ,"
            elif col_type in ('CO',):  # CLOB
                col_len = row['ColumnFormat'].replace('X(', '').replace(')', '')
                select_query = select_query + f"CAST (\"{row['ColumnName']}\" AS VARCHAR({col_len})) AS \"{row['ColumnName']}\" ,"
            elif col_type in ('N', 'D'):
                if (int(row['DecimalTotalDigits']) < 0) | (int(row['DecimalFractionalDigits']) < 0):
                    col_name = row['ColumnName']
                    col_type = next(field["data_type"] for field in self.target_schema if field["col_name"] == col_name)
                    match col_type:
                        case decimal if "decimal" in decimal:
                            col_precision = decimal.upper()
                        case string if "varchar" in decimal:
                            col_precision = string.upper()
                        case string if "char" in decimal:
                            col_precision = string.upper()
                        case other:
                            col_precision = self.type_map.get(other)
                    # print(f"{col_name} :::: {col_precision}")
                    select_query = select_query + f"CAST (\"{row['ColumnName']}\" AS {col_precision}) AS \"{row['ColumnName']}\" ,"
                else:
                    select_query = select_query + f"CAST (\"{row['ColumnName']}\" AS DECIMAL({int(row['DecimalTotalDigits'])}, {int(row['DecimalFractionalDigits'])})) AS \"{row['ColumnName']}\" ,"
            else:
                select_query = select_query + f"\"{row['ColumnName']}\" ,"

        # Return the select query string
        return select_query[:-1]

    def build_select_query(self):
        cols: list[dict] = self.source_interface.get_columns()
        return self.get_column_type(cols)

    def truncate_target_table(self) -> None:
        self.logger.info(f"Truncating target table {str(self.builder.target_table)}")
        catalog, schema, table = astuple(self.builder.target_table)
        self.target_interface.query(f"TRUNCATE TABLE `{catalog}`.{schema}.{table}")

    @abstractmethod
    def run_snapshot(self):
        pass


class RunnerError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
