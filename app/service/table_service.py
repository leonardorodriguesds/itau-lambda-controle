from datetime import datetime
from typing import List
from models.dto.table_dto import TableDTO
from models.tables import Tables
from exceptions.table_insert_error import TableInsertError
from repositories.table_repository import TableRepository
from service.dependency_service import DependencyService
from service.partition_service import PartitionService
from service.task_executor_service import TaskExecutorService

class TableService:
    def __init__(self, session):
        self.session = session
        self.table_repo = TableRepository(session)
        self.dependency_service = DependencyService(session)
        self.partition_service = PartitionService(session)
        self.task_executor_service = TaskExecutorService(session)

    def save_multiple_tables(self, tables_dto: List[TableDTO], user: str):
        """
        Salva múltiplas tabelas em uma transação única. 
        Apenas comita se todas as tabelas forem salvas com sucesso.
        """
        try:
            for table_dto in tables_dto:
                self.save_table(table_dto, user)
            self.session.commit()  
            return "All tables saved successfully."
        except Exception as e:
            self.session.rollback()  
            raise TableInsertError(f"Error saving multiple tables: {str(e)}")

    def save_table(self, table_dto: TableDTO, user: str):
        if not table_dto:
            raise TableInsertError("Table data is required.")

        if table_dto.id:
            table: Tables = self.table_repo.get_by_id(table_dto.id)
            if not table:
                raise TableInsertError(f"Table with id {table_dto.id} not found.")
            table.name = table_dto.name
            table.description = table_dto.description
            table.last_modified_by = user
            table.last_modified_at = datetime.now()
            table.requires_approval = table_dto.requires_approval
            self.table_repo.update(table)
        else:
            table = Tables(
                name=table_dto.name,
                description=table_dto.description,
                created_by=user,
                created_at=datetime.now(),
                requires_approval=table_dto.requires_approval
            )
            self.table_repo.save(table)

        self.partition_service.save_partitions(table.id, table_dto.partitions)
        self.dependency_service.save_dependencies(table.id, table_dto.dependencies)

        return f"Table '{table.name}' saved successfully."
