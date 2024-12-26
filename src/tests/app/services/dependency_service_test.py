import pytest
from unittest.mock import MagicMock
from src.itaufluxcontrol.models.dependencies import Dependencies
from src.itaufluxcontrol.models.dto.table_dto import DependencyDTO
from src.itaufluxcontrol.exceptions.table_insert_error import TableInsertError
from src.itaufluxcontrol.service.dependency_service import DependencyService


@pytest.fixture
def dependency_service():
    logger = MagicMock()
    table_repository = MagicMock()
    dependency_repository = MagicMock()
    service = DependencyService(logger, table_repository, dependency_repository)
    return service, logger, table_repository, dependency_repository


def test_save_dependencies_with_existing_dependency(dependency_service):
    service, logger, table_repository, dependency_repository = dependency_service
    dependency_repository.get_by_table_id.return_value = [Dependencies(table_id=1, dependency_id=2)]

    dependencies_dto = [DependencyDTO(dependency_id=3)]
    service.save_dependencies(1, dependencies_dto)

    assert dependency_repository.save.call_count == 1
    saved_dependency = dependency_repository.save.call_args[0][0]
    assert saved_dependency.table_id == 1
    assert saved_dependency.dependency_id == 3


def test_save_dependencies_with_nonexistent_dependency_name(dependency_service):
    service, logger, table_repository, dependency_repository = dependency_service
    dependency_repository.get_by_table_id.return_value = []
    table_repository.get_by_name.return_value = None

    dependencies_dto = [DependencyDTO(dependency_name="nonexistent_table")]

    with pytest.raises(TableInsertError, match="Dependency table 'nonexistent_table' not found."):
        service.save_dependencies(1, dependencies_dto)


def test_save_dependencies_with_dependency_name(dependency_service):
    service, logger, table_repository, dependency_repository = dependency_service
    dependency_repository.get_by_table_id.return_value = []
    table_repository.get_by_name.return_value = MagicMock(id=3)

    dependencies_dto = [DependencyDTO(dependency_name="existing_table")]
    service.save_dependencies(1, dependencies_dto)

    assert dependency_repository.save.call_count == 1
    saved_dependency = dependency_repository.save.call_args[0][0]
    assert saved_dependency.table_id == 1
    assert saved_dependency.dependency_id == 3



def test_save_dependencies_ignores_existing_dependency(dependency_service):
    service, logger, table_repository, dependency_repository = dependency_service
    dependency_repository.get_by_table_id.return_value = [Dependencies(table_id=1, dependency_id=2)]

    dependencies_dto = [DependencyDTO(dependency_id=2)]
    service.save_dependencies(1, dependencies_dto)

    dependency_repository.save.assert_not_called()
