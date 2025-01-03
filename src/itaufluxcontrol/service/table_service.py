from datetime import datetime
from logging import Logger
from typing import List, Optional

from injector import inject
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from src.itaufluxcontrol.service.task_table_service import TaskTableService
from src.itaufluxcontrol.service.table_execution_service import TableExecutionService
from src.itaufluxcontrol.models.dto.table_dto import TableDTO
from src.itaufluxcontrol.models.tables import Tables
from src.itaufluxcontrol.exceptions.table_insert_error import TableInsertError
from src.itaufluxcontrol.repositories.table_repository import TableRepository
from src.itaufluxcontrol.service.dependency_service import DependencyService
from src.itaufluxcontrol.service.partition_service import PartitionService
from src.itaufluxcontrol.service.task_executor_service import TaskExecutorService

class TableService:
    @inject
    def __init__(
        self, logger: Logger, 
        table_repository: TableRepository, 
        dependency_service: DependencyService, 
        partition_service: PartitionService, 
        task_executor_service: TaskExecutorService, 
        table_execution_service: TableExecutionService,
        task_table_service: TaskTableService
    ):
        self.logger = logger
        self.table_repository = table_repository
        self.dependency_service = dependency_service
        self.partition_service = partition_service
        self.task_executor_service = task_executor_service
        self.table_execution_service = table_execution_service
        self.task_table_service = task_table_service
        
    def query(self, **filters):
        self.logger.debug(f"[{self.__class__.__name__}] Querying tables with filters: [{filters}]")
        return self.table_repository.query(**filters)
        
    def find(self, table_id: Optional[str] = None, table_name: Optional[str] = None):
        self.logger.debug(f"[{self.__class__.__name__}] Finding table: [{table_id}] [{table_name}]")
        if table_id:
            table = self.table_repository.get_by_id(table_id)
            if not table:
                raise NotFoundError(f"Table with id [{table_id}] not found.")
            return table
        elif table_name:
            table = self.table_repository.get_by_name(table_name)
            if not table:
                raise NotFoundError(f"Table with name {table_name} not found.")
            return table
        else:
            raise RuntimeError("Table id or name is required.")

    def save_table(self, table_dto: TableDTO, user: str):
        self.logger.debug(f"[{self.__class__.__name__}] Saving table: [{table_dto}]")
        if not table_dto:
            raise TableInsertError("Table data is required.")

        if table_dto.id:
            table: Tables = self.table_repository.get_by_id(table_dto.id)
            if not table:
                raise TableInsertError(f"Table with id {table_dto.id} not found.")
            table.name = table_dto.name
            table.description = table_dto.description
            table.last_modified_by = user
            table.last_modified_at = datetime.now()
            table.requires_approval = table_dto.requires_approval
        else:
            table = Tables(
                name=table_dto.name,
                description=table_dto.description,
                created_by=user,
                created_at=datetime.now(),
                requires_approval=table_dto.requires_approval
            )
        
        self.table_repository.save(table)            
        self.table_repository.session.flush()  

        self.logger.debug(f"[{self.__class__.__name__}] Table ID after flush: {table.id}")

        self.partition_service.save_partitions(table.id, table_dto.partitions)
        self.dependency_service.save_dependencies(table.id, table_dto.dependencies)
        
        for task_dto in table_dto.tasks:
           self.task_table_service.save(task_dto, table.id)

        return f"Table '{table.name}' saved successfully."

    
    def get_latest(self, table_id: Optional[str] = None, table_name: Optional[str] = None):
        self.logger.debug(f"[{self.__class__.__name__}] Getting latest execution for table: [{table_id}] [{table_name}]")
        table: Tables = self.find(table_id, table_name)
        
        return self.table_execution_service.get_latest_execution(table.id)
        
    def find_by_dependency(self, table_id: int):
        self.logger.debug(f"[{self.__class__.__name__}] Finding tables by dependency: [{table_id}]")
        return self.table_repository.get_by_dependecy(table_id)
    
    def delete(self, table_id: int):
        self.logger.debug(f"[{self.__class__.__name__}] Deleting table: [{table_id}]")
        self.table_repository.soft_delete(table_id)
        return f"Table ['{table_id}'] deleted successfully."