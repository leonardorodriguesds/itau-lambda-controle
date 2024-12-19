import logging
import boto3
import pytest
from injector import Injector
from src.app.config.config import AppModule
from src.app.config.logger import logger
from src.app.provider.session_provider import SessionProvider
from src.app.service.table_service import TableService
from src.app.service.table_partition_exec_service import TablePartitionExecService
from src.app.service.event_bridge_scheduler_service import EventBridgeSchedulerService

@pytest.fixture
def injector():
    """Instancia o Injector para os testes."""
    return Injector([AppModule()])

def test_session_provider_binding(injector):
    """Testa se SessionProvider está configurado corretamente como singleton."""
    session_provider_1 = injector.get(SessionProvider)
    session_provider_2 = injector.get(SessionProvider)
    assert isinstance(session_provider_1, SessionProvider)
    assert session_provider_1 is session_provider_2  

def test_table_service_binding(injector):
    """Testa se TableService está configurado corretamente como singleton."""
    table_service_1 = injector.get(TableService)
    table_service_2 = injector.get(TableService)
    assert isinstance(table_service_1, TableService)
    assert table_service_1 is table_service_2  

def test_table_partition_exec_service_binding(injector):
    """Testa se TablePartitionExecService está configurado corretamente como singleton."""
    table_partition_exec_service_1 = injector.get(TablePartitionExecService)
    table_partition_exec_service_2 = injector.get(TablePartitionExecService)
    assert isinstance(table_partition_exec_service_1, TablePartitionExecService)
    assert table_partition_exec_service_1 is table_partition_exec_service_2

def test_event_bridge_scheduler_service_binding(injector):
    """Testa se EventBridgeSchedulerService está configurado corretamente como singleton."""
    event_bridge_scheduler_service_1 = injector.get(EventBridgeSchedulerService)
    event_bridge_scheduler_service_2 = injector.get(EventBridgeSchedulerService)
    assert isinstance(event_bridge_scheduler_service_1, EventBridgeSchedulerService)
    assert event_bridge_scheduler_service_1 is event_bridge_scheduler_service_2

def test_logger_binding(injector):
    """Testa se o logger está configurado corretamente."""
    logger_instance = injector.get(logging.Logger)
    assert logger_instance is logger 

def test_boto3_session_binding():
    injector = Injector()

    injector.binder.bind(
        boto3.Session,
        to=boto3.Session()
    )

    boto3_session_1 = injector.get(boto3.Session)
    boto3_session_2 = injector.get(boto3.Session)

    assert isinstance(boto3_session_1, boto3.Session)
    assert boto3_session_1 is boto3_session_2 
    assert boto3_session_1.region_name == boto3_session_2.region_name
    assert boto3_session_1.get_credentials() == boto3_session_2.get_credentials()
