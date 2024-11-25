from logging import Logger
from typing import Any, Dict

from repositories.table_execution_repository import TableExecutionRepository


class TableExecutionService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger
        self.table_execution_repository = TableExecutionRepository(session, logger)
        
    def create_execution(self, table_id: int, source: str):
        self.logger.debug(f"[{self.__class__.__name__}] Creating execution for table {table_id} with source {source}")
        return self.table_execution_repository.create_execution(table_id, source)
    
    def get_latest_execution(self, table_id: int):
        self.logger.debug(f"[{self.__class__.__name__}] Getting latest execution for table [{table_id}]")
        return self.table_execution_repository.get_latest_execution(table_id)
    
    def get_latest_execution_with_restrictions(self, table_id: int, required_partitions: Dict[str, Any]):
        self.logger.debug(f"[{self.__class__.__name__}] Getting latest execution for table [{table_id}] with restrictions: [{required_partitions}]")
        return self.table_execution_repository.get_latest_execution_with_restrictions(table_id, required_partitions)