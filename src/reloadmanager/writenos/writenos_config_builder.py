import random
from functools import cached_property
from datetime import datetime

from reloadmanager.generics.generic_config_builder import GenericConfigBuilder
from reloadmanager.mixins.secret_mixin import SecretMixin


class WriteNOSConfigBuilder(GenericConfigBuilder, SecretMixin):

    def __init__(
            self,
            source_table: str,
            target_table: str,
            where_clause: str = None,
            primary_key: str = None,
            lock_rows: bool = True
            # create_table_if_not_exists: bool = False
    ):
        super().__init__(source_table, target_table, where_clause, primary_key, lock_rows)
        self.aws_bucket = self.get_secret("AWS_BUCKET")

    @cached_property
    def id(self) -> str:
        return f"{self.source_table.table[:8]}{random.randint(100, 999)}"

    @cached_property
    def stage_root_dir(self) -> str:
        ts: str = datetime.now().strftime("%Y%m%d_%H%M%S_") + f"{datetime.now().microsecond // 1000:03d}"
        return f"reload-manager-stage/{self.target_table.catalog}/{self.source_table.as_path()}/migration_{self.id}_{ts}"
