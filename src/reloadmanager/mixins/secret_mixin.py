import os


class SecretMixin:

    _DBX_MARKERS = ("DATABRICKS_RUNTIME_VERSION", "DATABRICKS_CLUSTER_ID")

    @classmethod
    def _on_databricks(cls) -> bool:
        return any(os.getenv(v) for v in cls._DBX_MARKERS)

    @staticmethod
    def _dbutils():
        """
        Lazy-import dbutils so local dev boxes don’t choke.
        Works on jobs, notebooks, and SQL-warehouse clusters.
        """
        import pyspark
        from pyspark.dbutils import DBUtils

        return DBUtils(pyspark.sql.SparkSession.builder.getOrCreate())

    @classmethod
    def get_secret(cls, key: str, scope: str | None = None) -> str:

        try:
            return os.environ[key]
        except KeyError:
            if cls._on_databricks():
                dbutils = cls._dbutils()
                scope_name = scope or os.getenv("DBX_SECRET_SCOPE")
                if not scope_name:
                    raise RuntimeError(f"No secret scope found, please define under 'DBX_SECRET_SCOPE' env variable")
                try:
                    return dbutils.secrets.get(scope=scope_name, key=key)
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(
                        f"failed fetching '{key}' from scope '{scope_name}': {exc}"
                    ) from exc
            raise RuntimeError(f"Missing required env var: {key}")
