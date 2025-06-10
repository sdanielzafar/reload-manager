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
            primary_key: str = None,
            lock_rows: bool = True,
            create_table_if_not_exists: bool = False
    ):
        super().__init__(source_table, target_table, where_clause, primary_key, lock_rows, create_table_if_not_exists)

    @cached_property
    def id(self) -> str:
        return f"{self.source_table.table[:8]}{random.randint(100, 999)}"
