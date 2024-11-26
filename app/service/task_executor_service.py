from logging import Logger

from injector import inject
from models.task_executor import TaskExecutor
from repositories.task_executor_repository import TaskExecutorRepository

class TaskExecutorService:
    @inject
    def __init__(self, logger: Logger, repository: TaskExecutorRepository):
        self.logger = logger
        self.repository = repository
