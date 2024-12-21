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
from src.app.models.tables import Tables
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


def test_should_only_trigger_task_when_all_required_partitions_are_ready(test_injector):
    """
    Exemplo de teste para a rota /add_table,
    usando uma sessão SQLite in-memory.
    """
    event = {
        "httpMethod": "POST",
        "path": "/add_table",
        "body": json.dumps({
            "data": [
                {
                    "name": "tbjf001_op_pdz_prep",
                    "description": "Tabela de operações preparadas do PDZ",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        },
                        {
                            "name": "identificador_empresa",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                },
                {
                    "name": "tbjf002_op_plz_prep",
                    "description": "Tabela de operações preparadas do PLZ",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        },
                        {
                            "name": "identificador_empresa",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                },
                {
                    "name": "tb_op_enriquecido",
                    "description": "Tabela de operações preparadas do PLZ",
                    "requires_approval": True,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        },
                        {
                            "name": "identificador_empresa",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        }
                    ],
                    "dependencies": [
                        {
                            "dependency_name": "tbjf001_op_pdz_prep",
                            "is_required": True
                        },
                        {
                            "dependency_name": "tbjf002_op_plz_prep",
                            "is_required": True
                        }
                    ],
                    "tasks": [
                        {
                            "task_executor": "step_function_executor",
                            "alias": "op_enriquecido_step_function_executor",
                            "params": {
                                "table_name": "{{table.name}}",
                                "table_description": "{{table.description}}"
                            },
                            "debounce_seconds": 30
                        }
                    ]
                }
            ],
            "user": "lrcxpnu"
        })
    }
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    from src.app.models.task_executor import TaskExecutor
    session.add(TaskExecutor(alias="step_function_executor", method="step_function"))
    
    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 200
    
    tables = session.query(Tables).all()
    
    assert len(tables) == 3
    
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

    mock_boto_service = test_injector.get(BotoService)
    mock_scheduler_client = mock_boto_service.get_client('scheduler')

    register_response = lambda_handler(
        event=register_event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )
    
    assert register_response["statusCode"] == 200

    from src.app.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3), f"Esperava {(1 * 3)} execuções, mas encontrou {len(all_execs)}"

    from src.app.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 0, f"Esperava 0 agendamentos, mas encontrou {len(all_schedules)}"
    
def test_should_only_trigger_task_when_all_required_partitions_are_ready_with_optional_partitions(test_injector):
    """
    Exemplo de teste para a rota /add_table,
    usando uma sessão SQLite in-memory.
    """
    event = {
        "httpMethod": "POST",
        "path": "/add_table",
        "body": json.dumps({
            "data": [
                {
                    "name": "tbjf001_op_pdz_prep",
                    "description": "Tabela de operações preparadas do PDZ",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        },
                        {
                            "name": "identificador_empresa",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                },
                {
                    "name": "tbjf002_op_plz_prep",
                    "description": "Tabela de operações preparadas do PLZ",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        },
                        {
                            "name": "identificador_empresa",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                },
                {
                    "name": "tb_modalidade_prep",
                    "description": "Tabela de modalidades",
                    "requires_approval": False,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        }
                    ],
                    "dependencies": [],
                    "tasks": []
                },
                {
                    "name": "tb_op_enriquecido",
                    "description": "Tabela de operações preparadas do PLZ",
                    "requires_approval": True,
                    "partitions": [
                        {
                            "name": "ano_mes_referencia",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        },
                        {
                            "name": "versao_processamento",
                            "type": "int",
                            "is_required": True
                        },
                        {
                            "name": "identificador_empresa",
                            "type": "int",
                            "is_required": True,
                            "sync_column": True
                        }
                    ],
                    "dependencies": [
                        {
                            "dependency_name": "tbjf001_op_pdz_prep",
                            "is_required": True
                        },
                        {
                            "dependency_name": "tbjf002_op_plz_prep",
                            "is_required": True
                        },
                        {
                            "dependency_name": "tb_modalidade_prep",
                            "is_required": False
                        }
                    ],
                    "tasks": [
                        {
                            "task_executor": "step_function_executor",
                            "alias": "op_enriquecido_step_function_executor",
                            "params": {
                                "table_name": "{{table.name}}",
                                "table_description": "{{table.description}}"
                            },
                            "debounce_seconds": 30
                        }
                    ]
                }
            ],
            "user": "lrcxpnu"
        })
    }
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    from src.app.models.task_executor import TaskExecutor
    session.add(TaskExecutor(alias="step_function_executor", method="step_function"))
    
    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 200
    
    tables = session.query(Tables).all()
    
    assert len(tables) == 4
    
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
                },
                {
                    "table_name": "tbjf002_op_plz_prep",
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

    mock_boto_service = test_injector.get(BotoService)
    mock_scheduler_client = mock_boto_service.get_client('scheduler')

    register_response = lambda_handler(
        event=register_event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )
    
    assert register_response["statusCode"] == 200

    from src.app.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3), f"Esperava {(1 * 3) + (1 * 3)} execuções, mas encontrou {len(all_execs)}"

    from src.app.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamentos, mas encontrou {len(all_schedules)}"