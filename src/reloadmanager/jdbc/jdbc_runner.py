from reloadmanager.generics.generic_runner import GenericRunner, RunnerError
from reloadmanager.jdbc.jdbc_config_builder import JDBCConfigBuilder


class JDBCRunner(GenericRunner):
    builder: JDBCConfigBuilder

    def __init__(self, builder: JDBCConfigBuilder, source_interface=None, target_interface=None):
        super().__init__(builder, source_interface, target_interface)
        self.spark = self.target_interface.spark

    def pull(self, select_query: str, where_clause: str) -> list[tuple]:
        sql: str = f"{select_query} from {self.builder.source_table}{where_clause}"
        payload: list[tuple] = self.source_interface.safe_query(sql, headers=False)
        self.num_records = len(payload)
        return payload

    def append(self, payload: list[tuple], overwrite=False) -> None:
        self.spark.createDataFrame(payload) \
            .selectExpr([f"CAST({f['col_name']} AS {f['data_type']}) AS {f['col_name']}" for f in self.target_schema]) \
            .write.format("delta") \
            .mode("overwrite" if overwrite else "append") \
            .saveAsTable(f"{self.builder.target_table}")

    def run_snapshot(self):
        try:
            select_query = self.build_select_query()
            self.logger.info(f"Using query {select_query}")

            payload: list[tuple] = self.pull(select_query, self.builder.where_clause)

            # if there's no where clause, we can overwrite
            overwrite: bool = not bool(self.builder.where_clause)
            self.append(payload, overwrite)

        except Exception as e:
            raise RunnerError(f"JDBC table load failed with error: {repr(e)}")
