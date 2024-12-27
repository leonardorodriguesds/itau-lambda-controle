import json
import logging
from unittest.mock import MagicMock
import boto3
import pytest
from injector import Injector
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.itaufluxcontrol.config.config import AppModule
from src.itaufluxcontrol.config.logger import logger
from src.itaufluxcontrol.provider.database_provider import DatabaseProvider
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.service.table_service import TableService
from src.itaufluxcontrol.service.table_partition_exec_service import TablePartitionExecService
from src.itaufluxcontrol.service.event_bridge_scheduler_service import EventBridgeSchedulerService

class MockDatabaseProvider:
    def __init__(self, boto_service: MagicMock):
        """
        Mock do DatabaseProvider.
        """
        self.boto_service = boto_service
        self._configure_database()

    def _get_secret(self, secret_name):
        """
        Mock do método para retornar credenciais simuladas.
        """
        # Simula a resposta do Secrets Manager
        fake_secret = json.dumps({
            "username": "mock_user",
            "password": "mock_password",
            "host": "localhost",
            "port": "3306",
            "dbname": "mock_db"
        })
        return fake_secret

    def _configure_database(self):
        """
        Configura o banco de dados com credenciais simuladas.
        """
        secret = self._get_secret("mock_secret")
        credentials = json.loads(secret)

        db_user = credentials.get("username", "user")
        db_password = credentials.get("password", "password")
        db_host = credentials.get("host", "localhost")
        db_port = credentials.get("port", "3306")
        db_name = credentials.get("dbname", "lambdacontrole")

        database_url = (
            f"sqlite:///:memory:"  # Banco SQLite em memória para testes
        )

        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False},  # Necessário para SQLite em memória
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # Criação das tabelas para simulação
        from src.itaufluxcontrol.models.base import Base
        Base.metadata.create_all(self.engine)

    def set_charset(self, db):
        """
        Mock para simular configuração de charset no SQLite.
        """
        pass  # SQLite não requer configuração de charset

    def get_session(self):
        """
        Retorna uma sessão mockada do banco de dados.
        """
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()
            
@pytest.fixture
def mock_database_provider():
    """
    Retorna um mock do DatabaseProvider.
    """
    mock_boto_service = MagicMock()
    return MockDatabaseProvider(mock_boto_service)

@pytest.fixture
def injector(mock_database_provider):
    """Configura o Injector para testes com o mock do DatabaseProvider."""
    class TestModule(AppModule):
        def configure(self, binder):
            super().configure(binder)
            binder.bind(DatabaseProvider, to=mock_database_provider)

    return Injector([TestModule()])

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
