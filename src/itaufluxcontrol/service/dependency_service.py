from logging import Logger
from typing import List

from injector import inject
from src.itaufluxcontrol.models.dependencies import Dependencies
from src.itaufluxcontrol.models.dto.table_dto import DependencyDTO
from src.itaufluxcontrol.exceptions.table_insert_error import TableInsertError
from src.itaufluxcontrol.repositories.dependency_repository import DependencyRepository
from src.itaufluxcontrol.repositories.table_repository import TableRepository

class DependencyService:
    @inject
    def __init__(self, logger: Logger, repository: TableRepository, dependency_repository: DependencyRepository):
        self.logger = logger
        self.repository = repository
        self.dependency_repository = dependency_repository

    def save_dependencies(self, table_id: int, dependencies_dto: List[DependencyDTO]):
        existing_dependencies = {
            d.dependency_id for d in self.dependency_repository.get_by_table_id(table_id)
        }
        for dependency_data in dependencies_dto:
            if dependency_data.dependency_id and dependency_data.dependency_id not in existing_dependencies:
                dependency = Dependencies(
                    table_id=table_id,
                    dependency_id=dependency_data.dependency_id,
                    is_required=dependency_data.is_required
                )
                self.dependency_repository.save(dependency)
            elif not dependency_data.dependency_id and dependency_data.dependency_name:
                dependency_table = self.repository.get_by_name(dependency_data.dependency_name)
                if not dependency_table:
                    raise TableInsertError(f"Dependency table '{dependency_data.dependency_name}' not found.")
                dependency = Dependencies(
                    table_id=table_id,
                    dependency_id=dependency_table.id,
                    is_required=dependency_data.is_required
                )
                self.dependency_repository.save(dependency)
