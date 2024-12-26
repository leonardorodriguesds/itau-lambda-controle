import json
from unittest.mock import ANY
import pytest
from injector import Injector, Binder, singleton
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.itaufluxcontrol.config.constants import STATIC_APPROVE_STATUS_PENDING
from src.itaufluxcontrol.itaufluxcontrol import ItauFluxControl
from src.itaufluxcontrol.models.approval_status import ApprovalStatus
from src.itaufluxcontrol.models.base import Base  
from src.itaufluxcontrol.config.config import AppModule
from src.itaufluxcontrol.models.table_execution import TableExecution
from src.itaufluxcontrol.models.tables import Tables
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.service.boto_service import BotoService
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

@pytest.fixture
def itaufluxcontrol(test_injector):
    """
    Fixture que devolve uma instância de AppModule
    """
    from aws_lambda_powertools.event_handler import ApiGatewayResolver
    app_resolver = ApiGatewayResolver()
    return ItauFluxControl(
        injector=test_injector,
        app_resolver=app_resolver,
    )

def test_create_and_update_tables(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Testa a criação e atualização de tabelas.
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
                    "partitions": [
                        {
                            "name": "dt",
                            "type": "date"
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                }
            ],
            "user": "lrcxpnu"
        })
    }

    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    from src.itaufluxcontrol.models.tables import Tables 

    saved_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep').first()

    assert saved_table is not None
    assert saved_table.description == "Tabela teste"
    assert saved_table.requires_approval is False
    
    partitions = saved_table.partitions
    assert len(partitions) == 1
    assert partitions[0].name == "dt"
    assert partitions[0].type == "date"
    
    update_event = {
        "httpMethod": "PUT",
        "path": "/tables/1",
        "body": json.dumps({
            "data": [
                {
                    "id": saved_table.id,
                    "name": "tbjf001_op_pdz_prep",
                    "description": "Tabela teste atualizada",
                    "requires_approval": True,
                    "partitions": [
                        {
                            "name": "dt",
                            "type": "date"
                        },
                        {
                            "name": "hr",
                            "type": "int"
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                }
            ],
            "user": "lrcxpnu"
        })
    }
    
    response = itaufluxcontrol.process_event(update_event, None)
    
    assert response["statusCode"] == 200
    updated_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep').first()
    assert updated_table is not None
    assert updated_table.description == "Tabela teste atualizada"
    assert updated_table.requires_approval is True
    assert updated_table.last_modified_by == "lrcxpnu"
    partitions = updated_table.partitions
    assert len(partitions) == 2
    assert partitions[0].name == "dt"
    assert partitions[0].type == "date"
    assert partitions[1].name == "hr"
    assert partitions[1].type == "int"    
    
@pytest.mark.parametrize("filters, expected, expect_exception", [
    ({"name": "tbjf001_op_pdz_prep"}, {"count": 1, "name": "tbjf001_op_pdz_prep"}, False),
    ({"id": "1"}, {"count": 1, "id": 1}, False),
    ({}, {"count": 2}, False),
    ({"invalid_field": "value"}, None, True),  
    ({"name": "non_existing_table"}, {"count": 0}, False),
])
def test_get_all_tables_or_get_specific_table(test_injector, filters, expected, expect_exception, itaufluxcontrol: ItauFluxControl):
    """
    Testa a obtenção de todas as tabelas ou uma tabela específica com base nos filtros.
    """
    post_event = {
        "httpMethod": "POST",
        "path": "/tables",
        "body": json.dumps({
            "data": [
                {
                    "name": "tbjf001_op_pdz_prep",
                    "description": "Tabela teste",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "dt",
                            "type": "date"
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                },
                {
                    "name": "tbjf001_op_pdz_prep2",
                    "description": "Tabela teste 2",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "dt",
                            "type": "date"
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                }
            ],
            "user": "lrcxpnu"
        })
    }

    response = itaufluxcontrol.process_event(post_event, None)

    assert response["statusCode"] == 200, "Falha ao criar tabelas."

    get_event = {
        "httpMethod": "GET",
        "path": "/tables",
        "queryStringParameters": filters if filters else None
    }

    response = itaufluxcontrol.process_event(get_event, None)

    if expect_exception:
        assert response["statusCode"] == 500
    else:
        assert response["statusCode"] == 200, "Falha na obtenção das tabelas."

        try:
            body = json.loads(response["body"])
        except json.JSONDecodeError:
            pytest.fail("A resposta não é um JSON válido.")

        data = body if body else []

        assert isinstance(data, list), "O campo 'data' não é uma lista."

        assert len(data) == expected["count"], f"Esperado {expected['count']} resultados, obtido {len(data)}."

        if "name" in filters:
            if filters["name"] == "non_existing_table":
                assert len(data) == 0, f"Esperado 0 resultado, obtido {len(data)}."
            else:
                assert data[0]["name"] == expected["name"], f"Esperado nome '{expected['name']}', obtido '{data[0]['name']}'."
        if "id" in filters:
            assert data[0]["id"] == expected["id"], f"Esperado id '{expected['id']}', obtido '{data[0]['id']}'."

def test_delete_table(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Testa a exclusão de uma tabela.
    """
    create_event = {
        "httpMethod": "POST",
        "path": "/tables",
        "body": json.dumps({
            "data": [
                {
                    "name": "tbjf001_op_pdz_prep_delete_test",
                    "description": "Tabela teste para delete",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "dt",
                            "type": "date"
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                }
            ],
            "user": "lrcxpnu"
        })
    }

    create_response = itaufluxcontrol.process_event(create_event, None)

    assert create_response["statusCode"] == 200, "Falha ao criar a tabela para exclusão."

    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    created_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep_delete_test').first()

    assert created_table is not None, "A tabela criada não foi encontrada no banco de dados."

    table_id = created_table.id

    delete_event = {
        "httpMethod": "DELETE",
        "path": f"/tables/{table_id}",
        "pathParameters": {"table_id": str(table_id)},
        "body": None
    }

    delete_response = itaufluxcontrol.process_event(delete_event, None)

    assert delete_response["statusCode"] == 200, "Falha ao excluir a tabela."

    deleted_table = session.query(Tables).filter_by(id=table_id).first()

    assert deleted_table is not None, "A tabela excluída não foi encontrada no banco de dados."
    assert deleted_table.date_deleted is not None, "A data de exclusão não foi preenchida."

    get_event = {
        "httpMethod": "GET",
        "path": "/tables",
        "queryStringParameters": {"id": str(table_id)}
    }

    get_response = itaufluxcontrol.process_event(get_event, None)

    assert get_response["statusCode"] == 200, "Falha ao obter tabelas após exclusão."

    get_body = json.loads(get_response["body"])
    data = get_body if get_body else []

    assert isinstance(data, list), "O campo 'data' não é uma lista."
    assert len(data) == 0, "A tabela excluída ainda está presente na resposta GET."