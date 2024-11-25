from logging import Logger
from sqlalchemy.orm import Session
from repositories.generic_repository import GenericRepository
from models.task_executor import TaskExecutor

class TaskExecutorRepository(GenericRepository[TaskExecutor]):
    def __init__(self, session: Session, logger: Logger):
        super().__init__(session, TaskExecutor, logger)
        self.session = session
        self.logger = logger
