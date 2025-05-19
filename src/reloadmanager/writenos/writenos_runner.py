from dataclasses import astuple

from reloadmanager.generics.generic_runner import GenericRunner, RunnerError
from reloadmanager.writenos.writenos_config_builder import WriteNOSConfigBuilder


class WriteNOSRunner(GenericRunner):
    builder: WriteNOSConfigBuilder

    def __init__(self, builder: WriteNOSConfigBuilder, source_interface=None, target_interface=None):
        super().__init__(builder, source_interface, target_interface)

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

    def target_table_count(self) -> int:
        catalog, schema, table = astuple(self.builder.target_table)
        result: list[tuple] = self.target_interface.query(f"SELECT COUNT(1) FROM `{catalog}`.{schema}.{table}")
        return int(result[0][0])

    def copy_s3_files_into_delta(self, s3_path: str):

        # for VIEWs, the DDL might not be there
        if not self.target_schema:
            self.target_interface.spark.read.parquet(s3_path) \
                .limit(0) \
                .write.format("delta") \
                .save(str(self.builder.target_table))

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

    def run_snapshot(self, validate_counts: bool = False):

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
