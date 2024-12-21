from unittest import mock
import pytest
from unittest.mock import MagicMock
from src.app.service.task_table_service import TaskTableService
from src.app.models.dto.table_dto import TaskDTO
from src.app.models.task_table import TaskTable

@pytest.fixture
def task_table_service():
    logger = MagicMock()
    repository = MagicMock()
    task_executor_service = MagicMock()

    return TaskTableService(logger, repository, task_executor_service)

def test_find_existing_task(task_table_service):
    mock_task_table = MagicMock(spec=TaskTable)
    mock_task_table.id = 1
    mock_task_table.alias = "task1"
    task_table_service.repository.get_by_id.return_value = mock_task_table

    result = task_table_service.find(task_id=1)

    task_table_service.logger.debug.assert_called()
    task_table_service.repository.get_by_id.assert_called_once_with(1)
    assert result == mock_task_table

def test_find_non_existing_task(task_table_service):
    task_table_service.repository.get_by_id.return_value = None

    with pytest.raises(Exception) as exc_info:
        result = task_table_service.find(task_id=1)        

def test_save_new_task(task_table_service):
    mock_dto = TaskDTO(
        id=None,
        alias="task1",
        task_executor_id=None,
        task_executor="executor1",
        debounce_seconds=10,
        params={"key": "value"}
    )
    mock_executor = MagicMock()
    mock_executor.id = 2

    task_table_service.task_executor_service.find.return_value = mock_executor

    mock_task_table = MagicMock(spec=TaskTable)
    mock_task_table.id = 1
    mock_task_table.alias = "task1"
    mock_task_table.task_executor_id = 2

    task_table_service.repository.save.return_value = mock_task_table

    result = task_table_service.save(dto=mock_dto)

    task_table_service.logger.debug.assert_called()
    task_table_service.task_executor_service.find.assert_called_once_with(alias="executor1")
    task_table_service.repository.save.assert_called()
    assert result.alias == "task1"
    assert result.task_executor_id == 2

def test_save_existing_task(task_table_service):
    mock_dto = TaskDTO(
        id=1,
        alias="task1",
        task_executor_id=None,
        task_executor="executor1",
        debounce_seconds=10,
        params={"key": "value"}
    )
    mock_task_table = MagicMock(spec=TaskTable)
    mock_task_table.id = 1
    mock_task_table.alias = "old_task"
    mock_task_table.task_executor_id = 2

    task_table_service.repository.get_by_id.return_value = mock_task_table
    task_table_service.repository.save.return_value = mock_task_table

    result = task_table_service.save(dto=mock_dto)

    task_table_service.logger.debug.assert_called()
    task_table_service.repository.get_by_id.assert_called_once_with(1)
    task_table_service.repository.save.assert_called()
    assert result.alias == "task1"
