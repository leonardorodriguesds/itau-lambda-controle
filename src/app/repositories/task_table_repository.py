from logging import Logger
from injector import inject
from src.app.models.task_table import TaskTable
from src.app.provider.session_provider import SessionProvider
from src.app.repositories.generic_repository import GenericRepository


class TaskTableRepository(GenericRepository[TaskTable]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskTable, logger)
        self.session = session_provider.get_session()
        self.logger = logger