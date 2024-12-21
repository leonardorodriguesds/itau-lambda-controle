import json
import pytest
from injector import Injector, Binder, singleton
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.app.models.base import Base  # Ajuste o import para onde estiver seu Base
from lambda_function import lambda_handler
from src.app.config.config import AppModule
from src.app.models.task_executor import TaskExecutor
from src.app.provider.session_provider import SessionProvider
from src.app.service.boto_service import BotoService
from src.tests.providers.mock_scheduler_cliente_provider import MockBotoService
from src.tests.providers.mock_session_provider import TestSessionProvider


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
def test_injector(db_session):
    """
    Fixture que devolve um Injector que usa a sessão 
    em memória no lugar do SessionProvider real.
    """
    class TestModule(AppModule):
        def configure(self, binder: Binder) -> None:
            super().configure(binder)            
            binder.bind(BotoService, to=MockBotoService(), scope=singleton)
            binder.bind(SessionProvider, to=TestSessionProvider(db_session))

    return Injector([TestModule()])


def test_add_table_route(test_injector):
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

    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 200
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    from src.app.models.tables import Tables 

    saved_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep').first()

    assert saved_table is not None
    assert saved_table.description == "Tabela teste"
    assert saved_table.requires_approval is False
    
def test_add_table_route_with_partitions(test_injector):
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

    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 200
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    from src.app.models.tables import Tables 

    saved_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep').first()

    assert saved_table is not None
    assert saved_table.description == "Tabela teste"
    assert saved_table.requires_approval is False
    
    from src.app.models.partitions import Partitions
    saved_partition = session.query(Partitions).filter_by(name='dt').first()
    assert saved_partition is not None
    assert saved_partition.type == "date"
    
def test_add_table_route_with_partitions_and_tasks(test_injector):
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

    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 200

    from src.app.models.tables import Tables 

    saved_table = session.query(Tables).filter_by(name='tbjf001_op_pdz_prep').first()

    assert saved_table is not None
    assert saved_table.description == "Tabela teste"
    assert saved_table.requires_approval is False
    
    from src.app.models.partitions import Partitions
    saved_partition = session.query(Partitions).filter_by(name='dt').first()
    assert saved_partition is not None
    assert saved_partition.type == "date"
    
    from src.app.models.task_table import TaskTable
    saved_task = session.query(TaskTable).filter_by(alias='op_enriquecido_step_function_executor').first()
    
    assert saved_task is not None
    assert saved_task.task_executor.alias == "step_function_executor"
    assert saved_task.debounce_seconds == 30
    
def test_add_table_route_with_dependencies(test_injector):
    """
    Exemplo de teste para a rota /add_table,
    usando uma sessão SQLite in-memory.
    Verifica a criação de múltiplas tabelas, suas partições
    e as dependências em tb_op_enriquecido.
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
                    ]
                }
            ],
            "user": "lrcxpnu"
        })
    }
    
    session_provider = test_injector.get(SessionProvider)
    session = session_provider.get_session()

    response = lambda_handler(
        event=event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert response["statusCode"] == 200

    from src.app.models.tables import Tables
    from src.app.models.partitions import Partitions
    from src.app.models.dependencies import Dependencies

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

def test_add_table_and_register_executions(test_injector):
    """
    1) Cria múltiplas tabelas (rota /add_table).
    2) Verifica se foram salvas corretamente.
    3) Registra execução (rota /register_execution).
    4) Verifica se retornou HTTP 200.
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

    from src.app.models.tables import Tables
    from src.app.models.partitions import Partitions
    from src.app.models.dependencies import Dependencies
    from src.app.models.task_table import TaskTable

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

    register_response = lambda_handler(
        event=register_event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert register_response["statusCode"] == 200

    from src.app.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == (1 * 3) + (1 * 3) + (1 * 3) + (1 * 2), f"Esperava {(1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)} execuções, mas encontrou {len(all_execs)}"

    from src.app.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"
    
    register_response = lambda_handler(
        event=register_event,
        context=None,
        injected_injector=test_injector,
        debug=True
    )

    assert register_response["statusCode"] == 200

    from src.app.models.table_partition_exec import TablePartitionExec
    all_execs = session.query(TablePartitionExec).all()
    assert len(all_execs) == ((1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)) * 2, f"Esperava {((1 * 3) + (1 * 3) + (1 * 3) + (1 * 2)) * 2} execuções, mas encontrou {len(all_execs)}"

    from src.app.models.task_schedule import TaskSchedule
    all_schedules = session.query(TaskSchedule).all()
    
    assert len(all_schedules) == 1, f"Esperava 1 agendamento, mas encontrou {len(all_schedules)}"