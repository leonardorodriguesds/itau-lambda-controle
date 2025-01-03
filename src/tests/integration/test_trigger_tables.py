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

def test_should_only_trigger_task_when_all_required_partitions_are_ready(test_injector, itaufluxcontrol: ItauFluxControl):
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

    from src.itaufluxcontrol.models.task_executor import TaskExecutor
    session.add(TaskExecutor(alias="step_function_executor", method="step_function"))
    
    response = itaufluxcontrol.process_event(event, None)

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

    register_response = itaufluxcontrol.process_event(register_event, None)
    
    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3), f"Esperava {(1 * 3)} execuções, mas encontrou {len(all_execs)}"

    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 0, f"Esperava 0 agendamentos, mas encontrou {len(all_schedules)}"

def test_should_only_trigger_task_when_all_required_partitions_are_ready_with_optional_partitions(test_injector, itaufluxcontrol: ItauFluxControl):
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

    register_response = itaufluxcontrol.process_event(register_event, None)
    
    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3), f"Esperava {(1 * 3) + (1 * 3)} execuções, mas encontrou {len(all_execs)}"

    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamentos, mas encontrou {len(all_schedules)}"
    
@pytest.mark.parametrize(
    "params",
    [
        (None),
        ({
            "table_name": "{{table.name}}",
            "partitions": {
                "ano_mes_referencia": "{{partitions.ano_mes_referencia | int - 1}}",
                "identificador_empresa": "{{partitions.identificador_empresa}}"
            },
            "source": "{{execution.source}}",
            "task_alias": "{{task_table.alias}}"
        })
    ],
)
def test_should_trigger_last_execution_process_when_call_run_without_payload(test_injector, itaufluxcontrol: ItauFluxControl, params):
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
                                "table_description": "{{table.description}}",
                                "partitions": {
                                    "ano_mes_referencia": "{{partitions.ano_mes_referencia}}",
                                    "identificador_empresa": "{{partitions.identificador_empresa}}"
                                }
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
    session.add(TaskExecutor(alias="step_function_executor", method="stepfunction_process", identification="arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine"))
    
    response = itaufluxcontrol.process_event(event, None)

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

    register_response = itaufluxcontrol.process_event(register_event, None)
    
    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3), f"Esperava {(1 * 3) + (1 * 3)} execuções, mas encontrou {len(all_execs)}"

    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamentos, mas encontrou {len(all_schedules)}"
    
    _, first_value = next(iter(mock_scheduler_client._schedules.items()))
    trigger_event = json.loads(first_value["Target"]["Input"])
    
    trigger_event["body"] = json.dumps(trigger_event["body"])
    
    trigger_response = itaufluxcontrol.process_event(trigger_event, None)
    
    assert trigger_response["statusCode"] == 200
    
    table_tb_teste = session.query(Tables).filter_by(name="tb_op_enriquecido").first()
        
    expected_input = {
        "execution_id": ANY,
        "table_id": table_tb_teste.id,
        "source": ANY,
        "task_schedule_id": ANY,
        "date_time": ANY,
        "payload": {
            "table_name": "tb_op_enriquecido",
            "table_description": ANY,
            "partitions": {
                "ano_mes_referencia": "2405",
                "identificador_empresa": "0"
            }
        }
    }
    
    mock_step_function_client = mock_boto_service.get_client('stepfunctions')
    assert len(mock_step_function_client._executions) == 1, (
        "Esperava 1 execução de StepFunction, encontrou 0"
    )
    
    _, exec_info = next(iter(mock_step_function_client._executions.items()))
    
    assert exec_info["stateMachineArn"] == "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine", (
        f"ARN incorreto. Esperava=arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine, obteve={exec_info['stateMachineArn']}"
    )
    assert json.loads(exec_info["input"]) == expected_input, (
        f"Input incorreto. Esperava={expected_input}, obteve={json.loads(exec_info['input'])}"
    )
        
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
    assert len(all_execs) == (1 * 3) + (1 * 3) + (1 * 3), f"Esperava {(1 * 3) + (1 * 3) + (1 * 3)} execuções, mas encontrou {len(all_execs)}"
    
    table_op_enriquecido = session.query(Tables).filter(Tables.name == "tb_op_enriquecido").first()
    table_execution_op_enriquecido = session.query(TableExecution).filter(TableExecution.table_id == table_op_enriquecido.id).first()
    
    assert table_execution_op_enriquecido is not None
    assert table_execution_op_enriquecido.source == "glue"
    
    register_event = {
        "httpMethod": "POST",
        "path": "/run",
        "body": json.dumps({
            "table_name": "tb_op_enriquecido",
            "task_name": "op_enriquecido_step_function_executor",
            "params": params,
            "user": "lrcxpnu"
        })
    }
    
    run_response = itaufluxcontrol.process_event(register_event, None)
    
    assert run_response["statusCode"] == 200
    
    assert len(mock_step_function_client._executions) == 2, f"Esperava 2 execuções de StepFunction, encontrou {len(mock_step_function_client._executions)}"
    
    _, exec_info = next(reversed(mock_step_function_client._executions.items()))
    assert exec_info["stateMachineArn"] == "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine", (
        f"ARN incorreto. Esperava=arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine, obteve={exec_info['stateMachineArn']}"
    )
    
    if params is None:
        assert json.loads(exec_info["input"]) == expected_input, (
            f"Input incorreto. Esperava={expected_input}, obteve={json.loads(exec_info['input'])}"
        )
    else:
        assert json.loads(exec_info["input"]) != expected_input, (
            f"Input incorreto. Esperava={expected_input}, obteve={json.loads(exec_info['input'])}"
        )
        
        new_expected_input = expected_input.copy()
        new_expected_input["payload"] = {
            "table_name": "tb_op_enriquecido",
            "partitions": {
                "ano_mes_referencia": "2404",
                "identificador_empresa": "0"
            },
            "source": "glue",
            "task_alias": "op_enriquecido_step_function_executor"
        }
        
        assert json.loads(exec_info["input"]) == new_expected_input, (
            f"Input incorreto. Esperava={new_expected_input}, obteve={json.loads(exec_info['input'])}"
        )
        
def test_when_reject_execute_table_should_not_trigger_execution(test_injector, itaufluxcontrol: ItauFluxControl):
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
                                "table_description": "{{table.description}}",
                                "partitions": {
                                    "ano_mes_referencia": "{{partitions.ano_mes_referencia}}",
                                    "identificador_empresa": "{{partitions.identificador_empresa}}"
                                }
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
    session.add(TaskExecutor(alias="step_function_executor", method="stepfunction_process", identification="arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine"))
    
    response = itaufluxcontrol.process_event(event, None)

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

    register_response = itaufluxcontrol.process_event(register_event, None)
    
    assert register_response["statusCode"] == 200

    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3), f"Esperava {(1 * 3) + (1 * 3)} execuções, mas encontrou {len(all_execs)}"
    
    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamentos antes da aprovação, mas encontrou {len(all_schedules)}"
    
    approval_status = session.query(ApprovalStatus).filter_by(status=STATIC_APPROVE_STATUS_PENDING).first()
        
    assert approval_status is not None, "Status de aprovação não encontrado"
    
    approval_event = {
        "httpMethod": "POST",
        "path": "/reject",
        "body": json.dumps({
            "approval_status_id": approval_status.id,
            "user": "lrcxpnu"
        })
    }
    
    register_response = itaufluxcontrol.process_event(approval_event, None)

    assert register_response["statusCode"] == 200
    
    all_schedules = session.query(TaskSchedule).all()
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"
    assert len(mock_scheduler_client._schedules) == 0, f"Esperava 0 agendamento, mas encontrou {len(mock_scheduler_client._schedules)}"