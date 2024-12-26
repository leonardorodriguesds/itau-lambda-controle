import json
from unittest.mock import ANY
import pytest
from injector import Injector, Binder, singleton
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.app.models.base import Base  
from lambda_function import lambda_handler
from src.app.config.config import AppModule
from src.app.models.table_execution import TableExecution
from src.app.models.task_executor import TaskExecutor
from src.app.provider.session_provider import SessionProvider
from src.app.service.boto_service import BotoService
from src.tests.providers.mock_scheduler_cliente_provider import MockBotoService, MockEventsClient, MockGlueClient, MockLambdaClient, MockRequestsClient, MockSQSClient, MockSchedulerClient, MockStepFunctionClient
from src.tests.providers.mock_session_provider import TestSessionProvider

@pytest.fixture
def mock_boto_service():
    """
    Retorna uma instância do MockBotoService contendo mocks para:
      - Scheduler (EventBridge Scheduler)
      - Step Functions
      - SQS
      - Glue
      - Lambda
      - EventBridge (Events)
      - Requests (opcional, para o caso de api_process)
    """
    return MockBotoService(
        mock_scheduler_client=MockSchedulerClient(),
        step_function_client=MockStepFunctionClient(),
        sqs_client=MockSQSClient(),
        glue_client=MockGlueClient(),
        lambda_client=MockLambdaClient(),
        events_client=MockEventsClient(),
        requests_client=MockRequestsClient()
    )

@pytest.fixture
def db_session():
    """
    Fixture de banco de dados em memória:
      1. Cria engine SQLite em memória
      2. Cria as tabelas do projeto
      3. Cria a sessão
      4. Entrega a sessão para o teste
      5. Ao final do teste, fecha a sessão e dropa as tabelas
    """
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture
def test_injector(db_session, mock_boto_service):
    """
    Fixture que devolve um Injector que usa a sessão 
    em memória no lugar do SessionProvider real.
    """
    class TestModule(AppModule):
        def configure(self, binder: Binder) -> None:
            super().configure(binder)            
            binder.bind(BotoService, to=mock_boto_service, scope=singleton)
            binder.bind(SessionProvider, to=TestSessionProvider(db_session))

    return Injector([TestModule()])


def test_lambda_handler_execution_not_found(test_injector):
    """
    Testa o retorno de erro quando não encontra a execução
    """
    event = {
        "httpMethod": "POST",
        "path": "/random_route",
        "body": json.dumps({
            "data": [
                {
                    "name": "tbjf001_op_pdz_prep",
                    "description": "Tabela teste",
                    "requires_approval": False,
                    "partitions": [],
                    "dependencies": [],
                    "tasks": []
                }
            ],
            "user": "lrcxpnu"
        })
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == 404
    
def test_should_raise_exception_when_try_to_register_a_not_found_table(test_injector):
    """
    Testa o retorno de erro quando não encontra a tabela
    """
    register_event = {
        "httpMethod": "POST",
        "path": "/register_execution",
        "body": json.dumps({
            "data": [
                {
                    "table_name": "tbjf001_op_pdz_prep",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"},
                        {"partition_name": "identificador_empresa", "value": "0"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "lrcxpnu"
        })
    }

    response = lambda_handler(
        event=register_event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 500
    
def test_should_raise_exception_when_try_to_create_table_with_cyclic_dependency(test_injector):
    """
    Testa o retorno de erro quando não encontra a tabela
    """
    event = {
        "httpMethod": "POST",
        "path": "/tables",
        "body": json.dumps({
            "data": [
                {
                    "name": "tbjf001_op_pdz_prep",
                    "description": "Tabela teste",
                    "requires_approval": False,
                    "partitions": [],
                    "dependencies": [
                        {
                            "dependency_name": "tb_op_enriquecido",
                            "is_required": True
                        }
                    ],
                    "tasks": []
                },
                {
                    "name": "tbjf001_op_plz_prep",
                    "description": "Tabela teste",
                    "requires_approval": False,
                    "partitions": [],
                    "dependencies": [
                        {
                            "dependency_name": "tbjf001_op_pdz_prep",
                            "is_required": True
                        }
                    ],
                    "tasks": []
                },
                {
                    "name": "tb_op_enriquecido",
                    "description": "Tabela teste",
                    "requires_approval": False,
                    "partitions": [],
                    "dependencies": [
                        {
                            "dependency_name": "tbjf001_op_plz_prep",
                            "is_required": True
                        }
                    ],
                    "tasks": []
                }
            ],
            "user": "lrcxpnu"
        })
    }

    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 500