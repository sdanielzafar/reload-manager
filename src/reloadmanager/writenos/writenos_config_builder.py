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
            where_clause: str = None
    ):
        super().__init__(source_table, target_table, where_clause)
        self.aws_key = self.get_secret("AWS_KEY")
        self.aws_secret = self.get_secret("AWS_SECRET")
        self.aws_bucket = self.get_secret("AWS_BUCKET")

    @cached_property
    def id(self) -> str:
        return f"{self.source_table.table[:8]}{random.randint(100, 999)}"

    @cached_property
    def stage_root_dir(self) -> str:
        ts: str = datetime.now().strftime("%Y%m%d_%H%M%S_") + f"{datetime.now().microsecond // 1000:03d}"
        return f"reload-manager-stage/{self.target_table.catalog}/{self.id}/migration_{self.source_table.table}_{ts}"

    @staticmethod
    def _validate_extract_strategy(strategy: str) -> str:
        match strategy:
            case 'TPT':
                return "TPT"
            case 'WriteNOS':
                return "TERADATA_WRITE_NOS"
            case _:
                raise ValueError("'strategy' must be either 'TPT' or 'WriteNOS'")
