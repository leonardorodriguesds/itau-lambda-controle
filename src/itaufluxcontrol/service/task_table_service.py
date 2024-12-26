import json
from logging import Logger
from typing import Optional
from injector import inject
from aws_lambda_powertools.event_handler.exceptions import NotFoundError

from src.itaufluxcontrol.models.dto.table_dto import TaskDTO
from src.itaufluxcontrol.models.task_table import TaskTable
from src.itaufluxcontrol.repositories.task_table_repository import TaskTableRepository
from src.itaufluxcontrol.service.task_executor_service import TaskExecutorService


class TaskTableService:
    @inject
    def __init__(self, logger: Logger, repository: TaskTableRepository, task_executor_service: TaskExecutorService):
        self.logger = logger
        self.repository = repository
        self.task_executor_service = task_executor_service
        
    def find(self, task_id: Optional[int] = None, task_name: Optional[str] = None) -> TaskTable:
        self.logger.debug(f"[{self.__class__.__name__}] Finding task table by id: {task_id} or name: {task_name}")
        res = None
        if task_id:
            res = self.repository.get_by_id(task_id)
        elif task_name:
            res = self.repository.get_by_alias(task_name)
        else:
            raise Exception("Either task_id or task_name must be provided")
        if not res:
            raise NotFoundError(f"Task table with id {task_id} or name {task_name} not found")
        return res
    
    def save(self, dto: TaskDTO, table_id: Optional[int] = None) -> TaskTable:
        self.logger.debug(f"[{self.__class__.__name__}] Saving task table: [{dto}]")
        
        task_table: TaskTable = None
        if dto.id:
            task_table = self.repository.get_by_id(dto.id)
            if not task_table:
                raise Exception(f"Task table with id {dto.id} not found")
        else:
            task_table = TaskTable()
            
        task_table.alias = dto.alias
        if dto.task_executor_id:
            task_table.task_executor_id = dto.task_executor_id    
        else:
            task_table.task_executor_id = self.task_executor_service.find(alias=dto.task_executor).id
            
        task_table.debounce_seconds = dto.debounce_seconds
        task_table.table_id = table_id
        task_table.params = dto.params

        
        return self.repository.save(task_table)