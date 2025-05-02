import random
from functools import cached_property

from reloadmanager.generics.generic_config_builder import GenericConfigBuilder
from reloadmanager.mixins.secret_mixin import SecretMixin


class JDBCConfigBuilder(GenericConfigBuilder, SecretMixin):

    def __init__(
            self,
            source_table: str,
            target_table: str,
            where_clause: str = None,
            lock_rows: bool = True
    ):
        super().__init__(source_table, target_table, where_clause, lock_rows)

    @cached_property
    def id(self) -> str:
        return f"{self.source_table.table[:8]}{random.randint(100, 999)}"
