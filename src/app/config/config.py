import logging
import boto3
from injector import Binder, Injector, Module, singleton
from src.app.provider.boto3_session_provider import Boto3SessionProvider
from src.app.config.logger import logger
from src.app.provider.session_provider import SessionProvider
from src.app.service.table_service import TableService
from src.app.service.table_partition_exec_service import TablePartitionExecService
from src.app.service.event_bridge_scheduler_service import EventBridgeSchedulerService

class AppModule(Module):
    """Configuração das dependências para o Injector."""
    def configure(self, binder: Binder) -> None:
        binder.bind(SessionProvider, to=SessionProvider, scope=singleton)
        binder.bind(TableService, to=TableService, scope=singleton)
        binder.bind(TablePartitionExecService, to=TablePartitionExecService, scope=singleton)
        binder.bind(EventBridgeSchedulerService, to=EventBridgeSchedulerService, scope=singleton)
        binder.bind(logging.Logger, to=logger),
        binder.bind(boto3.Session, to=Boto3SessionProvider().provide_session(), scope=singleton)



injector = Injector([AppModule()])
