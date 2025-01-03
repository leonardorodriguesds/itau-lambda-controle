from logging import Logger
from injector import inject
from src.itaufluxcontrol.models.task_table import TaskTable
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.repositories.generic_repository import GenericRepository


class TaskTableRepository(GenericRepository[TaskTable]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskTable, logger)
        self.session = session_provider.get_session()
        self.logger = logger
        
    def get_by_alias(self, alias: str) -> TaskTable:
        return self.session.query(TaskTable).filter(TaskTable.alias == alias).first()