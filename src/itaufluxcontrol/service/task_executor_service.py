from logging import Logger
from typing import Optional

from injector import inject
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from src.itaufluxcontrol.models.dto.task_executor_dto import TaskExecutorDTO
from src.itaufluxcontrol.models.task_executor import TaskExecutor
from src.itaufluxcontrol.repositories.task_executor_repository import TaskExecutorRepository

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
                raise NotFoundError(f"Task executor with id [{task_executor_id}] not found.")
            return task_executor
        elif alias:
            task_executor = self.repository.get_by_alias(alias)
            if not task_executor:
                raise NotFoundError(f"Task executor with alias [{alias}] not found.")
            return task_executor
        else:
            raise Exception("Task executor id or alias is required.")
        
    def save(self, task_executor_dto: TaskExecutorDTO):
        self.logger.debug(f"[{self.__class__.__name__}] Saving task executor: [{task_executor_dto}]")
        return self.repository.save(TaskExecutor(**task_executor_dto.model_dump()))
    
    def delete(self, task_executor_id: int):
        self.logger.debug(f"[{self.__class__.__name__}] Deleting task executor: [{task_executor_id}]")
        return self.repository.soft_delete(task_executor_id)