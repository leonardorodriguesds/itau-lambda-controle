from logging import Logger
from models.task_executor import TaskExecutor
from repositories.task_executor_repository import TaskExecutorRepository

class TaskExecutorService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger
        self.repository = TaskExecutorRepository(session, logger)
