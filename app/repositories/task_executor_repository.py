from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from config.session_provider import SessionProvider
from repositories.generic_repository import GenericRepository
from models.task_executor import TaskExecutor

class TaskExecutorRepository(GenericRepository[TaskExecutor]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskExecutor, logger)
        self.session = session_provider.get_session()
        self.logger = logger
