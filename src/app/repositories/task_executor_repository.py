from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from src.app.provider.session_provider import SessionProvider
from src.app.repositories.generic_repository import GenericRepository
from src.app.models.task_executor import TaskExecutor

class TaskExecutorRepository(GenericRepository[TaskExecutor]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskExecutor, logger)
        self.session = session_provider.get_session()
        self.logger = logger

    def get_by_alias(self, alias: str) -> TaskExecutor:
        self.logger.debug(f"[{self.__class__.__name__}] Finding task executor by alias: [{alias}]")
        return self.session.query(TaskExecutor).filter(TaskExecutor.alias == alias).first()