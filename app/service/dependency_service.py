from typing import List
from models.dependencies import Dependencies
from models.dto.table_dto import DependencyDTO
from exceptions.table_insert_error import TableInsertError
from repositories.dependency_repository import DependencyRepository
from repositories.table_repository import TableRepository

class DependencyService:
    def __init__(self, session):
        self.dependency_repo = DependencyRepository(session)
        self.table_repo = TableRepository(session)

    def save_dependencies(self, table_id: int, dependencies_dto: List[DependencyDTO]):
        existing_dependencies = {
            d.dependency_id for d in self.dependency_repo.get_by_table_id(table_id)
        }
        for dependency_data in dependencies_dto:
            if dependency_data.dependency_id and dependency_data.dependency_id not in existing_dependencies:
                dependency = Dependencies(
                    table_id=table_id,
                    dependency_id=dependency_data.dependency_id
                )
                self.dependency_repo.save(dependency)
            elif not dependency_data.dependency_id and dependency_data.dependency_name:
                dependency_table = self.table_repo.get_by_name(dependency_data.dependency_name)
                if not dependency_table:
                    raise TableInsertError(f"Dependency table '{dependency_data.dependency_name}' not found.")
                dependency = Dependencies(
                    table_id=table_id,
                    dependency_id=dependency_table.id
                )
                self.dependency_repo.save(dependency)
