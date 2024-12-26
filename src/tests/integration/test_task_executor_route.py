import json
import pytest
from injector import Injector, Binder, singleton
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.itaufluxcontrol.itaufluxcontrol import ItauFluxControl
from src.itaufluxcontrol.models.base import Base  
from src.itaufluxcontrol.config.config import AppModule
from src.itaufluxcontrol.models.dto.task_executor_dto import TaskExecutorDTO
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.service.boto_service import BotoService
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

def test_create_service_executor(test_injector, itaufluxcontrol: ItauFluxControl):
    """
    Testa a criação de um serviço de executor de tarefas.
    """
    task_executor_dto = TaskExecutorDTO(alias="test", method="test", identification="test", target_role_arn="test")
    
    event = {
        "httpMethod": "POST",
        "path": "/task_executor",
        "body": json.dumps(task_executor_dto.model_dump())
    }
    
    response = itaufluxcontrol.process_event(event, None)

    assert response["statusCode"] == 200
    
    from src.itaufluxcontrol.models.task_executor import TaskExecutor
    tasks_executors = test_injector.get(SessionProvider).get_session().query(TaskExecutor).all()
    
    assert len(tasks_executors) == 1
    assert tasks_executors[0].alias == "test"
    assert tasks_executors[0].method == "test"
    assert tasks_executors[0].identification == "test"
    assert tasks_executors[0].target_role_arn == "test"