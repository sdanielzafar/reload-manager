from abc import ABC, abstractmethod

from reloadmanager.clients.generic_database_client import GenericDatabaseClient
from reloadmanager.generics.generic_config_builder import GenericConfigBuilder
from reloadmanager.clients.databricks_client_factory import get_dbx_client
from reloadmanager.writenos.teradata_interface import TeradataInterface
from reloadmanager.mixins.logging_mixin import LoggingMixin


class GenericRunner(ABC, LoggingMixin):

    def __init__(self, builder: GenericConfigBuilder, source_interface=None, target_interface=None):
        self.builder: GenericConfigBuilder = builder
        self.source_interface: TeradataInterface = source_interface if source_interface \
            else TeradataInterface(str(self.builder.source_table))
        self.target_interface: GenericDatabaseClient = target_interface if target_interface \
            else get_dbx_client()
        self.num_records: int = 0

    @abstractmethod
    def run_snapshot(self):
        pass

class RunnerError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
