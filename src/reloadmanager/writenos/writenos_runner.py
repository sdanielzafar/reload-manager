from functools import cached_property
from dataclasses import astuple

from reloadmanager.generics.generic_runner import GenericRunner, RunnerError
from reloadmanager.writenos.writenos_config_builder import WriteNOSConfigBuilder


class WriteNOSRunner(GenericRunner):
    builder: WriteNOSConfigBuilder

    def __init__(self, builder: WriteNOSConfigBuilder, source_interface=None, target_interface=None):
        super().__init__(builder, source_interface, target_interface)

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

    def export_nos(self, select_query: str, where_clause: str = None):
        """
        Export data from Teradata database and table to S3 in Parquet format.

        :param select_query: the select query to use
        :param where_clause: an optional where clause to limit the data
        :return:
        """

        where_sql = f" WHERE {where_clause}" if where_clause else ""
        self.logger.info(f"Processing - {str(self.builder.source_table)}")
        self.logger.info(f"Exporting to S3 bucket suffix {self.builder.stage_root_dir}")

        s3 = f"/s3/s3.amazonaws.com/{self.builder.aws_bucket}/{self.builder.stage_root_dir}/"
        query = f'''LOCKING ROW FOR ACCESS
SELECT *
FROM WRITE_NOS (
ON  ({select_query[:-1]} FROM {str(self.builder.source_table)}{where_sql})
USING
AUTHORIZATION({self.source_interface.td_user}.authAccess)
LOCATION('{s3}')
STOREDAS('PARQUET')
MAXOBJECTSIZE('16MB')
COMPRESSION('SNAPPY')
) AS d'''

        self.logger.debug(f'Running Write_NOS using query: \n{query}')

        # Execute the SQL query to export data to S3 in Parquet format
        result: list[dict] = self.source_interface.query(query, headers=True)

        # Fetch the results of the query execution
        if len(result) > 0:
            self.num_records = sum(int(r['RecordCount']) for r in result)

        return f"s3a://{self.builder.aws_bucket}/{self.builder.stage_root_dir}/"

    def truncate_target_table(self) -> None:
        self.logger.info(f"Truncating target table {str(self.builder.target_table)}")
        catalog, schema, table = astuple(self.builder.target_table)
        self.target_interface.query(f"TRUNCATE TABLE `{catalog}`.{schema}.{table}")

    def target_table_count(self) -> int:
        catalog, schema, table = astuple(self.builder.target_table)
        result: list[tuple] = self.target_interface.query(f"SELECT COUNT(1) FROM `{catalog}`.{schema}.{table}")
        return int(result[0][0])

    def copy_s3_files_into_delta(self, s3_path: str):

        select_statement = ", ".join(
            [f"CAST({field['col_name']} AS {field['data_type']}) AS {field['col_name']}"
             for field in self.target_schema])

        query = f"""
COPY INTO {str(self.builder.target_table)}
FROM (
    SELECT {select_statement}
    FROM '{s3_path}'
)
FILEFORMAT = PARQUET
COPY_OPTIONS ('force'='true','mergeSchema' = 'false')
        """
        self.logger.debug(f"Running COPY INTO query: {query}")

        self.target_interface.query(query)

    def run_snapshot(self, validate_counts: bool = True):

        try:
            select_query = self.build_select_query()
            self.logger.info(f"Using query {select_query}")

            s3_path = self.export_nos(select_query, self.builder.where_clause)

            if not self.builder.where_clause:
                self.truncate_target_table()

            if validate_counts:
                init_count = self.target_table_count()

            self.logger.info(
                f"Importing {self.num_records} rows from {s3_path} to table {str(self.builder.target_table)}")
            self.copy_s3_files_into_delta(s3_path)

            if validate_counts:
                rows_inserted = self.target_table_count() - init_count
                if rows_inserted != self.num_records:
                    raise RuntimeError(f"Expected {self.num_records} rows inserted, but found {rows_inserted} instead.")
        except Exception as e:
            raise RunnerError(f"Native WriteNOS failed with error: {repr(e)}")
