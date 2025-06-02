import traceback

from reloadmanager.generics.generic_runner import GenericRunner, RunnerError
from reloadmanager.jdbc.jdbc_config_builder import JDBCConfigBuilder


class JDBCRunner(GenericRunner):
    builder: JDBCConfigBuilder

    def __init__(self, builder: JDBCConfigBuilder, source_interface=None, target_interface=None):
        super().__init__(builder, source_interface, target_interface)
        self.spark = self.target_interface.spark

    def pull(self, select_query: str, where_clause: str) -> list[tuple]:
        where_sql = f" WHERE {where_clause}" if where_clause else ""
        sql: str = f"{select_query} from {self.builder.source_table}{where_sql}"
        payload: list[tuple] = self.source_interface.safe_query(sql, headers=True)
        self.num_records = len(payload)

        return payload
    
    @staticmethod
    def fix_str_types(t: str) -> str:
        match t:
            case t if t.lower().startswith(("varchar", "char", "timestamp", "date", "interval")):
                return "string"
            case _:
                return t

    def append(self, payload: list[tuple], overwrite=False) -> None:

        schema: str = ", ".join([f"{col['col_name']} {self.fix_str_types(col['data_type'])}" for col in self.target_schema])
        self.logger.debug(f"Using payload schema: {schema}")

        self.spark.createDataFrame(payload, schema=schema) \
            .selectExpr([f"CAST({f['col_name']} AS {f['data_type']}) AS {f['col_name']}" for f in self.target_schema]) \
            .write.format("delta") \
            .mode("overwrite" if overwrite else "append") \
            .saveAsTable(f"{self.builder.target_table}")
        
    def merge(self, payload: list[tuple], primary_key: str) -> None:

        schema: str = ", ".join(f"{col['col_name']} {self.fix_str_types(col['data_type'])}" for col in self.target_schema)
        self.logger.debug(f"Using payload schema: {schema}")

        spark_df = self.spark.createDataFrame(payload, schema=schema)
        spark_df.createOrReplaceTempView("payload_temp_view")

        target_table: str = self.builder.target_table
        pk_column_list: list[str] = [col.strip() for col in primary_key.split(",")]

        pk_conditions: str = " AND ".join([f"target.{col} = source.{col}" for col in pk_column_list])
        update_set: str = ", ".join([f"{col} = source.{col}" for col in spark_df.columns if col not in pk_column_list])
        insert_cols: str = ", ".join(spark_df.columns)
        insert_vals: str = ", ".join([f"source.{col}" for col in spark_df.columns])

        merge_sql: str = f"""
            MERGE INTO {target_table} AS target
            USING payload_temp_view AS source
            ON {pk_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """

        self.logger.info(f"Running MERGE SQL: {merge_sql}")

        self.target_interface.query(merge_sql)

    def run_snapshot(self):
        try:
            select_query = self.select_query
            self.logger.info(f"Using query {select_query}")

            payload: list[tuple] = self.pull(select_query, self.builder.where_clause)

            if self.builder.primary_key:
                self.merge(payload, self.builder.primary_key)
            else:
                # if there's no where clause, we can overwrite
                overwrite: bool = not bool(self.builder.where_clause)
                self.append(payload, overwrite)

        # except Exception as e:
        #     raise RunnerError(f"JDBC table load failed with error: {repr(e)}")

        except Exception as e:
            tb_str = traceback.format_exc()
            self.logger.error(f"Full traceback:\n{tb_str}")
            raise RunnerError(f"JDBC table load failed with error: {e.__class__.__name__}: {e}")
