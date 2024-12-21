import pytest
from unittest.mock import MagicMock

from src.app.exceptions.not_found_exception import NotFoundException
from src.app.exceptions.table_insert_error import TableInsertError
from src.app.models.dto.table_dto import TableDTO
from src.app.models.tables import Tables
from src.app.service.table_service import TableService


@pytest.fixture
def table_service():
    logger = MagicMock()
    table_repository = MagicMock()
    dependency_service = MagicMock()
    partition_service = MagicMock()
    task_executor_service = MagicMock()
    table_execution_service = MagicMock()
    task_table_service = MagicMock()
    
    return TableService(
        logger,
        table_repository,
        dependency_service,
        partition_service,
        task_executor_service,
        table_execution_service,
        task_table_service,
    )


def test_find_by_id(table_service):
    table_service.table_repository.get_by_id.return_value = Tables(id=1, name="Test Table")
    
    table = table_service.find(table_id=1)
    
    assert table.name == "Test Table"
    table_service.table_repository.get_by_id.assert_called_once_with(1)


def test_find_by_name(table_service):
    table_service.table_repository.get_by_name.return_value = Tables(id=1, name="Test Table")
    
    table = table_service.find(table_name="Test Table")
    
    assert table.name == "Test Table"
    table_service.table_repository.get_by_name.assert_called_once_with("Test Table")


def test_find_missing_id_and_name(table_service):
    with pytest.raises(RuntimeError) as excinfo:
        table_service.find()

def test_save_existing_table(table_service):
    table_service.table_repository.get_by_id.return_value = Tables(id=1, name="Test Table")
    table_dto = TableDTO(id=1, name="Updated Table", description="Updated Description", requires_approval=False)
    
    response = table_service.save_table(table_dto, "user1")
    
    assert "Updated Table" in response
    table_service.table_repository.update.assert_called_once()


def test_save_new_table(table_service):
    table_service.table_repository.get_by_id.return_value = None
    table_dto = TableDTO(name="New Table", description="New Description", requires_approval=True)
    
    response = table_service.save_table(table_dto, "user1")
    
    assert "New Table" in response
    table_service.table_repository.save.assert_called_once()


def test_get_latest_execution(table_service):
    table = Tables(id=1, name="Test Table")
    table_service.table_repository.get_by_id.return_value = table
    table_service.table_execution_service.get_latest_execution.return_value = {"execution_id": 123}
    
    execution = table_service.get_latest(table_id=1)
    
    assert execution["execution_id"] == 123
    table_service.table_execution_service.get_latest_execution.assert_called_once_with(1)


def test_find_by_dependency(table_service):
    dependencies = [Tables(id=2, name="Dependent Table")]
    table_service.table_repository.get_by_dependecy.return_value = dependencies
    
    result = table_service.find_by_dependency(table_id=1)
    
    assert len(result) == 1
    assert result[0].name == "Dependent Table"
    table_service.table_repository.get_by_dependecy.assert_called_once_with(1)
