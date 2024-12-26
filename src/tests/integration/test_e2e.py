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
from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
from src.itaufluxcontrol.models.tables import Tables
from src.itaufluxcontrol.models.task_executor import TaskExecutor
from src.itaufluxcontrol.models.task_schedule import TaskSchedule
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

def test_e2e_central(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Teste end-to-end para a rota /tables com dependências transitivas.
    Valida que as execuções são corretamente disparadas conforme as dependências.
    """
    # Evento inicial para criação das tabelas com dependências transitivas
    event = {
        "httpMethod": "POST",
        "path": "/tables",
        "body": json.dumps({
            "data": [
                {
                    "name": "tb_a",
                    "description": "Tabela A",
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
                    "name": "tb_b",
                    "description": "Tabela B",
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
                            "dependency_name": "tb_a",
                            "is_required": True
                        }
                    ],
                    "tasks": []
                },
                {
                    "name": "tb_c",
                    "description": "Tabela C",
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
                            "dependency_name": "tb_b",
                            "is_required": True
                        }
                    ],
                    "tasks": []
                },
                {
                    "name": "tb_d",
                    "description": "Tabela D",
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
                            "dependency_name": "tb_c",
                            "is_required": True
                        }
                    ],
                    "tasks": [
                        {
                            "task_executor": "step_function_executor",
                            "alias": "tb_d_step_function_executor",
                            "params": {
                                "table_name": "{{table.name}}",
                                "table_description": "{{table.description}}"
                            },
                            "debounce_seconds": 30
                        }
                    ]
                }
            ],
            "user": "usuario_teste"
        })
    }

    # Configuração da sessão e adição do TaskExecutor necessário
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    from src.itaufluxcontrol.models.task_executor import TaskExecutor
    session.add(TaskExecutor(alias="step_function_executor", method="step_function"))
    session.commit()

    # Chamada ao lambda_handler para criar as tabelas
    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200, f"Resposta inesperada: {response}"

    # Verificação das tabelas criadas
    from src.itaufluxcontrol.models.tables import Tables
    tables = session.query(Tables).all()
    assert len(tables) == 4, f"Esperava 4 tabelas, mas encontrou {len(tables)}"

    # Evento para registrar a execução da tabela A
    register_event_a = {
        "httpMethod": "POST",
        "path": "/register_execution",
        "body": json.dumps({
            "data": [
                {
                    "table_name": "tb_a",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"},
                        {"partition_name": "identificador_empresa", "value": "0"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "usuario_teste"
        })
    }

    # Mock do serviço Boto
    mock_boto_service = test_injector.get(BotoService)
    mock_scheduler_client = mock_boto_service.get_client('scheduler')

    # Chamada ao lambda_handler para registrar a execução da tabela A
    register_response_a = itaufluxcontrol.process_event(register_event_a, None)

    assert register_response_a["statusCode"] == 200, f"Resposta inesperada ao registrar tb_a: {register_response_a}"

    # Evento para registrar a execução da tabela B
    register_event_b = {
        "httpMethod": "POST",
        "path": "/register_execution",
        "body": json.dumps({
            "data": [
                {
                    "table_name": "tb_b",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"},
                        {"partition_name": "identificador_empresa", "value": "0"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "usuario_teste"
        })
    }

    # Chamada ao lambda_handler para registrar a execução da tabela B
    register_response_b = itaufluxcontrol.process_event(register_event_b, None)

    assert register_response_b["statusCode"] == 200, f"Resposta inesperada ao registrar tb_b: {register_response_b}"

    # Evento para registrar a execução da tabela C
    register_event_c = {
        "httpMethod": "POST",
        "path": "/register_execution",
        "body": json.dumps({
            "data": [
                {
                    "table_name": "tb_c",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"},
                        {"partition_name": "identificador_empresa", "value": "0"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "usuario_teste"
        })
    }

    # Chamada ao lambda_handler para registrar a execução da tabela C
    register_response_c = itaufluxcontrol.process_event(register_event_c, None)

    assert register_response_c["statusCode"] == 200, f"Resposta inesperada ao registrar tb_c: {register_response_c}"

    # Evento para registrar a execução da tabela D
    register_event_d = {
        "httpMethod": "POST",
        "path": "/register_execution",
        "body": json.dumps({
            "data": [
                {
                    "table_name": "tb_d",
                    "partitions": [
                        {"partition_name": "ano_mes_referencia", "value": "2405"},
                        {"partition_name": "versao_processamento", "value": "1"},
                        {"partition_name": "identificador_empresa", "value": "0"}
                    ],
                    "source": "glue"
                }
            ],
            "user": "usuario_teste"
        })
    }

    # Chamada ao lambda_handler para registrar a execução da tabela D
    register_response_d = itaufluxcontrol.process_event(register_event_d, None)

    assert register_response_d["statusCode"] == 200, f"Resposta inesperada ao registrar tb_d: {register_response_d}"

    # Verificação das execuções das partições
    from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    # Espera 4 tabelas * 3 partições cada
    assert len(all_execs) == (4 * 3), f"Esperava {(4 * 3)} execuções, mas encontrou {len(all_execs)}"

    # Verificação dos agendamentos de tarefas
    from src.itaufluxcontrol.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()

    # Considerando que as dependências transitivas foram atendidas, apenas a tabela D deve ter uma tarefa agendada
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"

    # Validação específica do agendamento da tabela D
    schedule = all_schedules[0]
    assert schedule.task_table.table.name == "tb_d", f"Esperava agendamento para 'tb_d', mas encontrou '{schedule.table_name}'"

    # Validação adicional das dependências transitivas
    # Verifica que as dependências foram corretamente resolvidas
    tb_d = session.query(Tables).filter_by(name="tb_d").first()
    assert tb_d is not None, "Tabela 'tb_d' não encontrada na sessão."
    dependencies = tb_d.dependencies
    assert len(dependencies) == 1, f"Esperava 1 dependência para 'tb_d', mas encontrou {len(dependencies)}"
    assert dependencies[0].dependency_table.name == "tb_c", f"Esperava dependência 'tb_c', mas encontrou '{dependencies[0].dependency_table.name}'"

    tb_c = session.query(Tables).filter_by(name="tb_c").first()
    assert tb_c is not None, "Tabela 'tb_c' não encontrada na sessão."
    dependencies_c = tb_c.dependencies
    assert len(dependencies_c) == 1, f"Esperava 1 dependência para 'tb_c', mas encontrou {len(dependencies_c)}"
    assert dependencies_c[0].dependency_table.name == "tb_b", f"Esperava dependência 'tb_b', mas encontrou '{dependencies_c[0].dependency_table.name}'"

    tb_b = session.query(Tables).filter_by(name="tb_b").first()
    assert tb_b is not None, "Tabela 'tb_b' não encontrada na sessão."
    dependencies_b = tb_b.dependencies
    assert len(dependencies_b) == 1, f"Esperava 1 dependência para 'tb_b', mas encontrou {len(dependencies_b)}"
    assert dependencies_b[0].dependency_table.name == "tb_a", f"Esperava dependência 'tb_a', mas encontrou '{dependencies_b[0].dependency_table.name}'"

    tb_a = session.query(Tables).filter_by(name="tb_a").first()
    assert tb_a is not None, "Tabela 'tb_a' não encontrada na sessão."
    dependencies_a = tb_a.dependencies
    assert len(dependencies_a) == 0, f"Esperava 0 dependências para 'tb_a', mas encontrou {len(dependencies_a)}"