from logging import Logger
from typing import Optional

from injector import inject
from src.app.models.task_executor import TaskExecutor
from src.app.repositories.task_executor_repository import TaskExecutorRepository

class TaskExecutorService:
    @inject
    def __init__(self, logger: Logger, repository: TaskExecutorRepository):
        self.logger = logger
        self.repository = repository

    def find(self, task_executor_id: Optional[int] = None, alias: Optional[str] = None):
        self.logger.debug(f"[{self.__class__.__name__}] Finding task executor: [{task_executor_id}] [{alias}]")
        if task_executor_id:
            task_executor = self.repository.get_by_id(task_executor_id)
            if not task_executor:
                raise Exception(f"Task executor with id [{task_executor_id}] not found.")
            return task_executor
        elif alias:
            task_executor = self.repository.get_by_alias(alias)
            if not task_executor:
                raise Exception(f"Task executor with alias [{alias}] not found.")
            return task_executor
        else:
            raise Exception("Task executor id or alias is required.")