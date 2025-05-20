from abc import ABC, abstractmethod
from dataclasses import astuple
from functools import cached_property

from pyspark.sql.utils import AnalysisException

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

    @property
    def target_table_exists(self) -> bool:
        return bool(self.target_interface.query(
            f"SHOW TABLES IN {self.builder.target_table.catalog_schema} LIKE '{self.builder.target_table.table}';"
        ))

    def create_ddl(self) -> None:

        ddl_query: str = f"CREATE TABLE {str(self.builder.target_table)} ("

        # Iterate over each row in the DataFrame containing column metadata
        for row in self.source_interface.get_columns():
            col_type: str = row['Type'].strip()
            col_name: str = row['Column Name'].strip()

            # Determine how to handle different column types
            if col_type in ('SZ', 'MI', 'DH', 'DM', 'DS', 'DY', 'HM', 'HS', 'AT', 'TZ', 'CV', 'CF', 'CO', 'JN'):
                ddl_query += f"{col_name} STRING, "
            elif col_type in ('DA',):
                ddl_query += f"{col_name} DATE, "
            elif col_type in ('TS',):
                ddl_query += f"{col_name} TIMESTAMP, "
            elif col_type in ('I', 'I1', 'I2'):
                ddl_query += f"{col_name} INTEGER, "
            elif col_type in ('I8',):
                ddl_query += f"{col_name} LONG, "
            elif col_type in ('D', 'N'):
                # for most numeric and decimal columns, we can just cast using precision and scale.
                # but some types in Teradata are numeric and don't have these, so we need to handle dynamically
                if (int(row['Decimal Total Digits']) < 0) | (int(row['Decimal Fractional Digits']) < 0):
                    ddl_query += f"{col_name} STRING, "
                else:
                    ddl_query += f"{col_name} DECIMAL({int(row['Decimal Total Digits'])}, {int(row['Decimal Fractional Digits'])})), "
            elif col_type in ('F',):
                ddl_query += f"{col_name} DOUBLE, "
            else:
                raise Exception(
                    f"Unable to create DDL from {str(self.builder.source_table)}, found unexepected type: {col_type}"
                )

        # get rid of last comma
        ddl_query_fmt: str = ddl_query[:-2]

        ddl_query_fmt += ") USING DELTA;"
        self.logger.debug(f"Creating table with DDL: `{ddl_query_fmt}`")

        self.target_interface.query(ddl_query_fmt)

    @property
    def select_query(self) -> str:
        """
        Generates SQL select query and a dictionary of column names with their respective lengths/types.
        :return: Select query
        """

        if not self.target_table_exists:
            self.logger.info(f"TABLE or VIEW '{self.builder.target_table}' not found. Attempting to create DDL..")
            self.create_ddl()

        # Initialize the select query string
        select_query = "SELECT "

        # Iterate over each row in the DataFrame containing column metadata
        for row in self.source_interface.get_columns():
            col_type: str = row['Type'].strip()
            col_name: str = row['Column Name'].strip()

            # Determine how to handle different column types
            if col_type in ('TS', 'SZ', 'MI', 'DH', 'DM', 'DS', 'DY', 'HM', 'HS', 'AT', 'TZ'):
                select_query += f"CAST (\"{col_name}\" AS VARCHAR({row['Max Length']})) AS \"{col_name}\", "
            elif col_type in ('DA',):
                select_query += f"CAST (CAST (\"{col_name}\" AS DATE format 'YYYY-MM-DD') AS VARCHAR(10))  AS \"{col_name}\", "
            elif col_type in ('CF', 'CO'):
                col_len = row['Format'].replace('X(', '').replace(')', '').strip()
                select_query += f"CAST (\"{col_name}\" AS VARCHAR({col_len})) AS \"{col_name}\", "
            elif col_type in ('N', 'D'):
                # for most numeric and decimal columns, we can just cast using precision and scale.
                # but some types in Teradata are numeric and don't have these, so we need to handle dynamically
                if (int(row['Decimal Total Digits']) < 0) | (int(row['Decimal Fractional Digits']) < 0):
                    col_type = next(
                        field["data_type"] for field in self.target_schema if field["col_name"] == col_name
                    )
                    match col_type:
                        case decimal if "decimal" in decimal:
                            col_precision = decimal.upper()
                        case string if "varchar" in string:
                            col_precision = string.upper()
                        case char if "char" in char:
                            col_precision = char.upper()
                        case other:
                            col_precision = self.type_map.get(other)
                    select_query += f"CAST (\"{col_name}\" AS {col_precision}) AS \"{col_name}\", "
                else:
                    select_query += f"CAST (\"{col_name}\" AS DECIMAL({int(row['Decimal Total Digits'])}, " \
                                    f"{int(row['Decimal Fractional Digits'])})) AS \"{col_name}\", "
            else:
                select_query += f"\"{col_name}\", "

        # Return the select query string, but get rid of last comma
        return select_query[:-2]

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
