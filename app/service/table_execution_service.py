from logging import Logger

from repositories.table_execution_repository import TableExecutionRepository


class TableExecutionService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger
        self.table_execution_repository = TableExecutionRepository(session, logger)
        
    def create_execution(self, table_id: int, source: str):
        self.logger.debug(f"[TableService] Creating execution for table {table_id} with source {source}")
        return self.table_execution_repository.create_execution(table_id, source)
    
    def get_latest_execution(self, table_id: int):
        self.logger.debug(f"[TableService] Getting latest execution for table [{table_id}]")
        return self.table_execution_repository.get_latest_execution(table_id)