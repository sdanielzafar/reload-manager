from dataclasses import astuple
from textwrap import dedent

from reloadmanager.generics.generic_runner import GenericRunner, RunnerError
from reloadmanager.writenos.writenos_config_builder import WriteNOSConfigBuilder


class WriteNOSRunner(GenericRunner):
    builder: WriteNOSConfigBuilder

    def __init__(self, builder: WriteNOSConfigBuilder, source_interface=None, target_interface=None):
        super().__init__(builder, source_interface, target_interface)
        self.spark = self.target_interface.spark

    def export_nos(self, select_query: str, where_clause: str = None) -> str:
        """
        Export data from Teradata database and table to S3 in Parquet format.

        :param select_query: the select query to use
        :param where_clause: an optional where clause to limit the data
        :return: path where data is placed on cloud storage
        """

        where_sql = f" WHERE {where_clause}" if where_clause else ""
        self.logger.info(f"Processing - {str(self.builder.source_table)}")
        self.logger.info(f"Exporting to S3 bucket suffix {self.builder.stage_root_dir}")

        s3 = f"/s3/s3.amazonaws.com/{self.builder.aws_bucket}/{self.builder.stage_root_dir}/"

        full_query: str = f"{select_query} FROM {str(self.builder.source_table)}{where_sql}"
        self.logger.info(f"Using query {full_query}")

        query = dedent(f'''LOCKING ROW FOR ACCESS
            SELECT *
            FROM WRITE_NOS (
            ON  ({full_query})
            USING
            AUTHORIZATION({self.source_interface.td_user}.authAccess)
            LOCATION('{s3}')
            STOREDAS('PARQUET')
            MAXOBJECTSIZE('16MB')
            COMPRESSION('SNAPPY')
            ) AS d;''')

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
    
    @staticmethod
    def fix_str_types(t: str) -> str:
        match t:
            case t if t.lower().startswith(("varchar", "char", "timestamp", "date", "interval")):
                return "string"
            case _:
                return t

    def append(self, payload: str, overwrite: bool = False):
        if not overwrite:
            self.delete_from_target_table()

        select_statement = ", ".join(
            [f"CAST(`{field['col_name']}` AS {field['data_type']}) AS `{field['col_name']}`"
             for field in self.target_schema])

        query = dedent(f"""
            COPY INTO {str(self.builder.target_table)}
            FROM (
            SELECT {select_statement}
            FROM '{payload}'
            )
            FILEFORMAT = PARQUET
            COPY_OPTIONS ('force'='true','mergeSchema' = 'false')
        """)
        self.logger.debug(f"Running COPY INTO query: {query}")

        self.target_interface.query(query)

    def run_snapshot(self, validate_counts: bool = False):

        try:
            select_query = self.select_query

            s3_path = self.export_nos(select_query, self.builder.where_clause)

            if not self.builder.where_clause:
                self.truncate_target_table()
            
            if validate_counts:
                init_count = self.target_table_count()

            self.logger.info(
                f"Importing {self.num_records} rows from {s3_path} to table {str(self.builder.target_table)}")
            
            if self.builder.primary_key:
                schema: str = ", ".join(
                    f"{col['col_name']} {self.fix_str_types(col['data_type'])}" for col in self.target_schema)
                self.spark_df = self.spark.read.schema(schema).parquet(s3_path)
                self.spark_df.createOrReplaceTempView("payload_temp_view")
                self.merge()
            else:
                # if there's no where clause, we can overwrite
                overwrite: bool = not bool(self.builder.where_clause)
                self.append(s3_path, overwrite)

            if validate_counts:
                rows_inserted = self.target_table_count() - init_count
                if rows_inserted != self.num_records:
                    raise RuntimeError(f"Expected {self.num_records} rows inserted, but found {rows_inserted} instead.")
        except Exception as e:
            raise RunnerError(f"Native WriteNOS failed with error: {repr(e)}")
