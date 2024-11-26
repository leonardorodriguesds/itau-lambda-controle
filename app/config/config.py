import logging
from injector import Binder, Injector, Module, singleton
from config.logger import logger
from config.session_provider import SessionProvider
from service.table_service import TableService
from service.table_partition_exec_service import TablePartitionExecService
from service.event_bridge_scheduler_service import EventBridgeSchedulerService

class AppModule(Module):
    """Configuração das dependências para o Injector."""
    def configure(self, binder: Binder) -> None:
        binder.bind(SessionProvider, to=SessionProvider, scope=singleton)
        binder.bind(TableService, to=TableService, scope=singleton)
        binder.bind(TablePartitionExecService, to=TablePartitionExecService, scope=singleton)
        binder.bind(EventBridgeSchedulerService, to=EventBridgeSchedulerService, scope=singleton)
        binder.bind(logging.Logger, to=logger)

injector = Injector([AppModule()])
