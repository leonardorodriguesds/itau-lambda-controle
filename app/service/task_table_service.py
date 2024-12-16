import json
from logging import Logger
from typing import Optional
from injector import inject

from models.dto.table_dto import TaskDTO
from models.task_table import TaskTable
from repositories.task_table_repository import TaskTableRepository
from service.task_executor_service import TaskExecutorService


class TaskTableService:
    @inject
    def __init__(self, logger: Logger, repository: TaskTableRepository, task_executor_service: TaskExecutorService):
        self.logger = logger
        self.repository = repository
        self.task_executor_service = task_executor_service
        
    def find(self, task_id: int) -> TaskTable:
        self.logger.debug(f"[{self.__class__.__name__}] Finding task table: [{task_id}]")
        return self.repository.get_by_id(task_id)
    
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