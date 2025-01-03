import json
from unittest.mock import ANY
import pytest
from injector import Injector, Binder, singleton
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.itaufluxcontrol.config.constants import STATIC_APPROVE_STATUS_APPROVED, STATIC_APPROVE_STATUS_PENDING, STATIC_SCHEDULE_COMPLETED
from src.itaufluxcontrol.itaufluxcontrol import ItauFluxControl
from src.itaufluxcontrol.models.approval_status import ApprovalStatus
from src.itaufluxcontrol.models.base import Base  
from src.itaufluxcontrol.config.config import AppModule
from src.itaufluxcontrol.models.table_execution import TableExecution
from src.itaufluxcontrol.models.task_executor import TaskExecutor
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

def test_add_table_route(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Exemplo de teste para a rota /tables,
    usando uma sessão SQLite in-memory.
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
    
def test_add_table_route_with_partitions(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Exemplo de teste para a rota /tables,
    usando uma sessão SQLite in-memory.
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
    
    from src.itaufluxcontrol.models.partitions import Partitions
    saved_partition = session.query(Partitions).filter_by(name='dt').first()
    assert saved_partition is not None
    assert saved_partition.type == "date"
    
def test_add_table_route_with_partitions_and_tasks(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Exemplo de teste para a rota /tables,
    usando uma sessão SQLite in-memory.
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
    
    session.add(TaskExecutor(alias="step_function_executor", method="step_function"))

    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200

    from src.itaufluxcontrol.models.tables import Tables 

    saved_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep').first()

    assert saved_table is not None
    assert saved_table.description == "Tabela teste"
    assert saved_table.requires_approval is False
    
    from src.itaufluxcontrol.models.partitions import Partitions
    saved_partition = session.query(Partitions).filter_by(name='dt').first()
    assert saved_partition is not None
    assert saved_partition.type == "date"
    
    from src.itaufluxcontrol.models.task_table import TaskTable
    saved_task = session.query(TaskTable).filter_by(alias='op_enriquecido_step_function_executor').first()
    
    assert saved_task is not None
    assert saved_task.task_executor.alias == "step_function_executor"
    assert saved_task.debounce_seconds == 30
    
def test_add_table_route_with_dependencies(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Exemplo de teste para a rota /tables,
    usando uma sessão SQLite in-memory.
    Verifica a criação de múltiplas tabelas, suas partições
    e as dependências em tb_op_enriquecido.
    """
    event = {
        "httpMethod": "POST",
        "path": "/tables",
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
                    ]
                }
            ],
            "user": "lrcxpnu"
        })
    }
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200

    from src.itaufluxcontrol.models.tables import Tables
    from src.itaufluxcontrol.models.partitions import Partitions
    from src.itaufluxcontrol.models.dependencies import Dependencies

    all_tables = session.query(Tables).all()
    assert len(all_tables) == 4, f"Esperava 4 tabelas, mas obteve {len(all_tables)}"

    tb1 = session.query(Tables).filter_by(name="tbjf001_op_pdz_prep").first()
    assert tb1 is not None
    assert tb1.description == "Tabela de operações preparadas do PDZ"
    assert tb1.requires_approval is False

    partitions_tb1 = session.query(Partitions).filter_by(table_id=tb1.id).all()
    assert len(partitions_tb1) == 3

    tb2 = session.query(Tables).filter_by(name="tbjf002_op_plz_prep").first()
    assert tb2 is not None
    assert tb2.description == "Tabela de operações preparadas do PLZ"
    assert tb2.requires_approval is False
    partitions_tb2 = session.query(Partitions).filter_by(table_id=tb2.id).all()
    assert len(partitions_tb2) == 3

    tb3 = session.query(Tables).filter_by(name="tb_modalidade_prep").first()
    assert tb3 is not None
    assert tb3.description == "Tabela de modalidades"
    assert tb3.requires_approval is False
    partitions_tb3 = session.query(Partitions).filter_by(table_id=tb3.id).all()
    assert len(partitions_tb3) == 2

    tb4 = session.query(Tables).filter_by(name="tb_op_enriquecido").first()
    assert tb4 is not None
    assert tb4.description == "Tabela de operações preparadas do PLZ"
    assert tb4.requires_approval is True
    partitions_tb4 = session.query(Partitions).filter_by(table_id=tb4.id).all()
    assert len(partitions_tb4) == 3

    table_deps = session.query(Dependencies).filter_by(table_id=tb4.id).all()
    assert len(table_deps) == 3, f"Esperava 3 dependências, obteve {len(table_deps)}"

    expected_deps = {
        "tbjf001_op_pdz_prep": True,
        "tbjf002_op_plz_prep": True,
        "tb_modalidade_prep": False
    }

    for dep in table_deps:
        assert dep.dependency_table.name in expected_deps, (
            f"Dependency {dep.dependency_table.name} não esperada"
        )
        assert dep.is_required == expected_deps[dep.dependency_table.name], (
            f"Dependência {dep.dependency_table.name} deveria ter is_required="
            f"{expected_deps[dep.dependency_table.name]}"
        )

def test_add_table_and_register_executions(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    1) Cria múltiplas tabelas (rota /tables).
    2) Verifica se foram salvas corretamente.
    3) Registra execução (rota /register_execution).
    4) Verifica se retornou HTTP 200.
    """

    event = {
        "httpMethod": "POST",
        "path": "/tables",
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

    from src.itaufluxcontrol.models.task_executor import TaskExecutor
    session.add(TaskExecutor(alias="step_function_executor", method="step_function"))

    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200

    from src.itaufluxcontrol.models.tables import Tables
    from src.itaufluxcontrol.models.partitions import Partitions
    from src.itaufluxcontrol.models.dependencies import Dependencies
    from src.itaufluxcontrol.models.task_table import TaskTable

    all_tables = session.query(Tables).all()
    assert len(all_tables) == 4, f"Esperava 4 tabelas, mas encontrou {len(all_tables)}"

    tb1 = session.query(Tables).filter_by(name="tbjf001_op_pdz_prep").first()
    assert tb1 is not None
    assert tb1.description == "Tabela de operações preparadas do PDZ"
    assert tb1.requires_approval is False

    partitions_tb1 = session.query(Partitions).filter_by(table_id=tb1.id).all()
    assert len(partitions_tb1) == 3

    tb4 = session.query(Tables).filter_by(name="tb_op_enriquecido").first()
    assert tb4 is not None
    assert tb4.requires_approval is True
    partitions_tb4 = session.query(Partitions).filter_by(table_id=tb4.id).all()
    assert len(partitions_tb4) == 3

    table_deps = session.query(Dependencies).filter_by(table_id=tb4.id).all()
    assert len(table_deps) == 3

    expected_deps = {
        "tbjf001_op_pdz_prep": True,
        "tbjf002_op_plz_prep": True,
        "tb_modalidade_prep": False
    }
    for dep in table_deps:
        assert dep.dependency_table.name in expected_deps, f"Dependency {dep.dependency_table.name} não esperada"
        assert dep.is_required == expected_deps[dep.dependency_table.name]

    saved_task = session.query(TaskTable).filter_by(alias='op_enriquecido_step_function_executor').first()
    assert saved_task is not None
    assert saved_task.task_executor.alias == "step_function_executor"
    assert saved_task.debounce_seconds == 30

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
                    "table_name": "tbjf001_op_pdz_prep",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "2"},
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
                },
                {
                    "table_name": "tb_modalidade_prep",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "lrcxpnu"
        })
    }

    mock_boto_service = test_injector.get(BotoService)
    mock_scheduler_client = mock_boto_service.get_client('scheduler')

    register_response = itaufluxcontrol.process_event(register_event, None)

    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3) + (1 * 3) + (1 * 2), f"Esperava {(1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)} execuções, mas encontrou {len(all_execs)}"

    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"
    
    register_response = itaufluxcontrol.process_event(register_event, None)

    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == ((1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)) * 2, f"Esperava {((1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)) * 2} execuções, mas encontrou {len(all_execs)}"

    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"

@pytest.mark.parametrize(
    "executor_method, identification, requires_approval",
    [
        ("stepfunction_process", "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine", True),
        ("sqs_process", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", True),
        ("glue_process", "my-glue-job-name", True),
        ("lambda_process", "arn:aws:lambda:us-east-1:123456789012:function:my-lambda", True),
        ("eventbridge_process", "my.event.source", True),
        ("api_process", "https://fake.api/my-endpoint", True),
        ("stepfunction_process", "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine", False),
        ("sqs_process", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", False),
        ("glue_process", "my-glue-job-name", False),
        ("lambda_process", "arn:aws:lambda:us-east-1:123456789012:function:my-lambda", False),
        ("eventbridge_process", "my.event.source", False),
        ("api_process", "https://fake.api/my-endpoint", False),
    ],
)
def test_add_table_and_register_executions_and_trigger_process(
    test_injector, 
    mock_boto_service,
    itaufluxcontrol: ItauFluxControl,
    executor_method,
    identification,
    requires_approval
):
    event = {
        "httpMethod": "POST",
        "path": "/tables",
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
                    "requires_approval": requires_approval,
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
                            "task_executor": "mock_executor",
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

    from src.itaufluxcontrol.models.task_executor import TaskExecutor
    
    task_execution = TaskExecutor(alias="mock_executor", method=executor_method, identification=identification)
    
    session.add(task_execution)

    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200

    from src.itaufluxcontrol.models.tables import Tables
    from src.itaufluxcontrol.models.partitions import Partitions
    from src.itaufluxcontrol.models.dependencies import Dependencies
    from src.itaufluxcontrol.models.task_table import TaskTable

    all_tables = session.query(Tables).all()
    assert len(all_tables) == 4, f"Esperava 4 tabelas, mas encontrou {len(all_tables)}"

    tb1 = session.query(Tables).filter_by(name="tbjf001_op_pdz_prep").first()
    assert tb1 is not None
    assert tb1.description == "Tabela de operações preparadas do PDZ"
    assert tb1.requires_approval is False
    partitions_tb1 = session.query(Partitions).filter_by(table_id=tb1.id).all()
    assert len(partitions_tb1) == 3

    tb4 = session.query(Tables).filter_by(name="tb_op_enriquecido").first()
    assert tb4 is not None
    assert tb4.requires_approval is requires_approval
    partitions_tb4 = session.query(Partitions).filter_by(table_id=tb4.id).all()
    assert len(partitions_tb4) == 3

    table_deps = session.query(Dependencies).filter_by(table_id=tb4.id).all()
    assert len(table_deps) == 3

    expected_deps = {
        "tbjf001_op_pdz_prep": True,
        "tbjf002_op_plz_prep": True,
        "tb_modalidade_prep": False
    }
    for dep in table_deps:
        assert dep.dependency_table.name in expected_deps, f"Dependency {dep.dependency_table.name} não esperada"
        assert dep.is_required == expected_deps[dep.dependency_table.name]

    saved_task = session.query(TaskTable).filter_by(alias='op_enriquecido_step_function_executor').first()
    assert saved_task is not None
    assert saved_task.task_executor.alias == "mock_executor"
    assert saved_task.debounce_seconds == 30

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
                    "table_name": "tbjf001_op_pdz_prep",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "2"},
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
                },
                {
                    "table_name": "tb_modalidade_prep",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "lrcxpnu"
        })
    }

    mock_scheduler_client = mock_boto_service.get_client('scheduler')

    register_response = itaufluxcontrol.process_event(register_event, None)

    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3) + (1 * 3) + (1 * 2), f"Esperava {(1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)} execuções, mas encontrou {len(all_execs)}"

    # chama novamente para validar que só cria 1 schedule
    register_response = itaufluxcontrol.process_event(register_event, None)

    assert register_response["statusCode"] == 200

    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == ((1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)) * 2, f"Esperava {((1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)) * 2} execuções, mas encontrou {len(all_execs)}"
    
    if requires_approval:
        approval_status = session.query(ApprovalStatus).filter_by(status=STATIC_APPROVE_STATUS_PENDING).first()
        
        assert approval_status is not None, "Status de aprovação não encontrado"
        
        approval_event = {
            "httpMethod": "POST",
            "path": "/approve",
            "body": json.dumps({
                "approval_status_id": approval_status.id,
                "user": "lrcxpnu"
            })
        }
        
        register_response = itaufluxcontrol.process_event(approval_event, None)

        assert register_response["statusCode"] == 200
        
        approval_status_approved = session.query(ApprovalStatus).filter_by(status=STATIC_APPROVE_STATUS_APPROVED).first()
        assert approval_status_approved is not None
        assert approval_status_approved.id == approval_status.id
        
    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    task_schedule = all_schedules[0]
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"
    assert len(mock_scheduler_client._schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(mock_scheduler_client._schedules)}"

    table_execution = (
        session.query(TableExecution)
        .order_by(TableExecution.id.desc())
        .first()
    )

    execution_source = table_execution.source
    saved_task_id = saved_task.id
    saved_task_alias = saved_task.alias
    schedule_id = task_schedule.id
    schedule_alias = task_schedule.schedule_alias
    schedule_unique_alias = task_schedule.unique_alias
    db_seconds = saved_task.debounce_seconds
    
    body_content = {
        "execution": {
            "id": ANY,
            "source": execution_source,
            "timestamp": ANY
        },
        "task_table": {
            "id": saved_task_id,
            "alias": saved_task_alias
        },
        "task_schedule": {
            "id": schedule_id,
            "schedule_alias": schedule_alias,
            "unique_alias": schedule_unique_alias
        },
        "partitions": {}
    }

    expected_trigger_event = {
        "httpMethod": "POST",
        "path": "/trigger",
        "body": body_content,
        "metadata": {
            "unique_alias": schedule_unique_alias,
            "debounce_seconds": db_seconds
        }
    }

    _, first_value = next(iter(mock_scheduler_client._schedules.items()))
    trigger_event = json.loads(first_value["Target"]["Input"])

    assert trigger_event == expected_trigger_event   
    
    trigger_event["body"] = json.dumps(trigger_event["body"])
    
    trigger_response = itaufluxcontrol.process_event(trigger_event, None)
    
    assert trigger_response["statusCode"] == 200
    
    table_tb_teste = session.query(Tables).filter_by(name="tb_op_enriquecido").first()
        
    expected_input = {
        "execution_id": ANY,
        "table_id": table_tb_teste.id,
        "source": execution_source,
        "task_schedule_id": schedule_id,
        "date_time": ANY,
        "payload": {
            "table_name": "tb_op_enriquecido",
            "table_description": ANY
        }
    }
    
    if executor_method == "stepfunction_process":
        mock_step_function_client = mock_boto_service.get_client('stepfunctions')
        assert len(mock_step_function_client._executions) == 1, (
            "Esperava 1 execução de StepFunction, encontrou 0"
        )
        execution_name, exec_info = next(iter(mock_step_function_client._executions.items()))
        
        assert exec_info["stateMachineArn"] == identification, (
            f"ARN incorreto. Esperava={identification}, obteve={exec_info['stateMachineArn']}"
        )
        assert json.loads(exec_info["input"]) == expected_input, (
            f"Input incorreto. Esperava={expected_input}, obteve={json.loads(exec_info['input'])}"
        )

    elif executor_method == "sqs_process":
        mock_sqs_client = mock_boto_service.get_client('sqs')
        assert len(mock_sqs_client._messages) == 1, (
            "Esperava 1 mensagem enviada ao SQS, encontrou 0"
        )
        message = mock_sqs_client._messages[0]
        assert message["QueueUrl"] == identification, (
            f"QueueUrl incorreto. Esperava={identification}, obteve={message['QueueUrl']}"
        )
        body = json.loads(message["MessageBody"])
        assert body == expected_input, (
            f"MessageBody incorreto. Esperava={expected_input}, obteve={body}"
        )

    elif executor_method == "glue_process":
        mock_glue_client = mock_boto_service.get_client('glue')
        assert len(mock_glue_client._job_runs) == 1, (
            "Esperava 1 job run do Glue, encontrou 0"
        )
        job_run = mock_glue_client._job_runs[0]
        assert job_run["JobName"] == identification, (
            f"JobName incorreto. Esperava={identification}, obteve={job_run['JobName']}"
        )
        assert job_run["Arguments"]["payload"] == expected_input["payload"], (
            f"Arguments payload incorreto. Esperava={expected_input['payload']}, obteve={job_run['Arguments']['payload']}"
        )

    elif executor_method == "lambda_process":
        mock_lambda_client = mock_boto_service.get_client('lambda')
        assert len(mock_lambda_client._invocations) == 1, (
            "Esperava 1 invocação de Lambda, encontrou 0"
        )
        invocation = mock_lambda_client._invocations[0]
        assert invocation["FunctionName"] == identification, (
            f"FunctionName incorreto. Esperava={identification}, obteve={invocation['FunctionName']}"
        )
        payload_dict = json.loads(invocation["Payload"])
        assert payload_dict == expected_input, (
            f"Payload da Lambda incorreto. Esperava={expected_input}, obteve={payload_dict}"
        )

    elif executor_method == "eventbridge_process":
        mock_events_client = mock_boto_service.get_client('events')
        assert len(mock_events_client._put_events) == 1, (
            "Esperava 1 evento publicado no EventBridge, encontrou 0"
        )
        event_entry = mock_events_client._put_events[0]
        assert event_entry["Source"] == identification, (
            f"Source do evento incorreto. Esperava={identification}, obteve={event_entry['Source']}"
        )
        detail = json.loads(event_entry["Detail"])
        assert detail == expected_input, (
            f"Detail do evento incorreto. Esperava={expected_input}, obteve={detail}"
        )

    elif executor_method == "api_process":
        mock_requests_client = mock_boto_service.get_client('requests')
        assert len(mock_requests_client._posts) == 1, (
            "Esperava 1 chamada HTTP via requests.post, encontrou 0"
        )
        post_call = mock_requests_client._posts[0]
        assert post_call["url"] == identification, (
            f"URL da chamada API incorreto. Esperava={identification}, obteve={post_call['url']}"
        )
        sent_json = post_call["json"]
        assert sent_json == expected_input, (
            f"JSON enviado para API incorreto. Esperava={expected_input}, obteve={sent_json}"
        )

    else:
        pytest.fail(f"Executor method desconhecido: {executor_method}")
        
    register_event = {
        "httpMethod": "POST",
        "path": "/register_execution",
        "body": json.dumps({
            "data": [
                {
                    "table_name": "tb_op_enriquecido",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"},
                        {"partition_name": "identificador_empresa", "value": "0"}
                    ],
                    "source": "glue",
                    "task_schedule_id": schedule_id
                }
            ],
            "user": "lrcxpnu"
        })
    }

    mock_boto_service = test_injector.get(BotoService)
    mock_scheduler_client = mock_boto_service.get_client('scheduler')

    register_response = itaufluxcontrol.process_event(register_event, None)
    
    assert register_response["statusCode"] == 200
    
    all_schedules = session.query(TaskSchedule).all()
    task_schedule = all_schedules[0]
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"
    assert task_schedule.status == STATIC_SCHEDULE_COMPLETED, f"Esperava status '{STATIC_SCHEDULE_COMPLETED}', mas encontrou {task_schedule.status}"

    print(f"Teste para {executor_method} passou com sucesso")
    