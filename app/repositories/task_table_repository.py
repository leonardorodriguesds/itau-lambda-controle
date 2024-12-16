from logging import Logger
from injector import inject
from models.task_table import TaskTable
from provider.session_provider import SessionProvider
from repositories.generic_repository import GenericRepository


class TaskTableRepository(GenericRepository[TaskTable]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskTable, logger)
        self.session = session_provider.get_session()
        self.logger = logger