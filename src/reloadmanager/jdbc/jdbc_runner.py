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

    def append(self, payload: list[tuple], overwrite=False) -> None:

        def fix_str_types(t: str) -> str:
            match t:
                case t if t.lower().startswith("varchar"):
                    return "string"
                case t if t.lower().startswith("char"):
                    return "string"
                case t if t.lower().startswith("timestamp"):
                    return "string"
                case t if t.lower().startswith("date"):
                    return "string"
                case t if t.lower().startswith("interval"):
                    return "string"
                case _:
                    return t

        schema: str = ", ".join([f"`{col['col_name']}` {fix_str_types(col['data_type'])}" for col in self.target_schema])

        self.logger.debug(f"Using payload schema: {schema}")

        self.spark.createDataFrame(payload, schema=schema) \
            .selectExpr([f"CAST(`{f['col_name']}` AS {f['data_type']}) AS `{f['col_name']}`" for f in self.target_schema]) \
            .write.format("delta") \
            .mode("overwrite" if overwrite else "append") \
            .saveAsTable(f"{self.builder.target_table}")

    def run_snapshot(self):
        try:
            select_query = self.select_query
            self.logger.info(f"Using query {select_query}")

            payload: list[tuple] = self.pull(select_query, self.builder.where_clause)

            # if there's no where clause, we can overwrite
            overwrite: bool = not bool(self.builder.where_clause)
            self.append(payload, overwrite)

        # except Exception as e:
        #     raise RunnerError(f"JDBC table load failed with error: {repr(e)}")

        except Exception as e:
            tb_str = traceback.format_exc()
            self.logger.error(f"Full traceback:\n{tb_str}")
            raise RunnerError(f"JDBC table load failed with error: {e.__class__.__name__}: {e}")
