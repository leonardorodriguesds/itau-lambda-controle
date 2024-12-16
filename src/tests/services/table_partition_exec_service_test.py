import pytest
from unittest.mock import ANY, MagicMock, call, patch
from datetime import datetime
from src.app.service.table_partition_exec_service import TablePartitionExecService
from src.app.models.table_partition_exec import TablePartitionExec
from src.app.models.tables import Tables
from src.app.models.table_execution import TableExecution
from src.app.models.dto.table_partition_exec_dto import TablePartitionExecDTO, PartitionDTO
from src.app.exceptions.table_insert_error import TableInsertError

@pytest.fixture
def mock_services():
    """Mock all dependencies for TablePartitionExecService."""
    return {
        "logger": MagicMock(),
        "repository": MagicMock(),
        "table_service": MagicMock(),
        "table_execution_service": MagicMock(),
        "partition_service": MagicMock(),
        "event_bridge_scheduler_service": MagicMock(),
        "cloudwatch_service": MagicMock(),
    }

@pytest.fixture
def service(mock_services):
    """Create an instance of TablePartitionExecService with mocked dependencies."""
    return TablePartitionExecService(**mock_services)

def test_get_by_execution(service, mock_services):
    """Test that `get_by_execution` calls the repository correctly."""
    mock_services["repository"].get_by_execution.return_value = []

    execution_id = 1
    result = service.get_by_execution(execution_id)

    assert result == []
    mock_services["repository"].get_by_execution.assert_called_once_with(execution_id)

def test_trigger_tables_with_no_execution_dependencies(service, mock_services):
    """Test that `trigger_tables` triggers the correct dependent tables."""
    mock_table = MagicMock(id=1, name="MainTable")
    mock_services["table_service"].find.return_value = mock_table
    mock_task_table_1 = MagicMock(id=1)
    mock_task_table_2 = MagicMock(id=2)
    mock_services["table_service"].find_by_dependency.return_value = [
        Tables(id=2, name="DependentTable1", task_table=[mock_task_table_1,]),
        Tables(id=3, name="DependentTable2", task_table=[mock_task_table_2,]),
    ]
    mock_table_execution = MagicMock(id=10, table_id=1)
    mock_services["table_execution_service"].get_latest_execution.return_value = mock_table_execution
    mock_services["repository"].get_by_execution.side_effect = lambda execution_id: [
        MagicMock(partition=MagicMock(name="Partition1"), value="val1"),
        MagicMock(partition=MagicMock(name="Partition2"), value="val2"),
    ] if execution_id == 10 else []

    service.trigger_tables(mock_table.id)

    mock_services["table_service"].find.assert_called_once_with(table_id=1)
    mock_services["table_service"].find_by_dependency.assert_called_once_with(1)
    assert mock_services["event_bridge_scheduler_service"].register_or_postergate_event.call_count == 2
    
    expected_calls = [
        ((mock_task_table_1, mock_table_execution, ANY, ANY),),
        ((mock_task_table_2, mock_table_execution, ANY, ANY),),
    ]
    mock_services["event_bridge_scheduler_service"].register_or_postergate_event.assert_has_calls(expected_calls, any_order=True)
    
def test_trigger_tables_with_execution_dependencies(service, mock_services):
    mock_table = MagicMock(id=1, name="MainTable")
    mock_services["table_service"].find.return_value = mock_table
    mock_table_execution = MagicMock(id=10, table_id=1)
    mock_dependency_execution = MagicMock(id=11, table_id=2)
    mock_task_table_1 = MagicMock(id=1)
    mock_task_table_2 = MagicMock(id=2)
    mock_services["table_service"].find_by_dependency.return_value = [
        Tables(id=2, name="DependentTable1", task_table=[mock_task_table_1]),
        Tables(id=3, name="DependentTable2", task_table=[mock_task_table_2]),
    ]

    def get_mock_last_execution(table_id):
        if table_id == 1:
            return mock_table_execution
        elif table_id == 2:
            return mock_dependency_execution
        elif table_id == 3:
            return None

    mock_services["table_execution_service"].get_latest_execution.side_effect = get_mock_last_execution
    mock_services["table_execution_service"].get_latest_execution_with_restrictions.side_effect = lambda table_id, current_partitions: get_mock_last_execution(table_id)

    def get_mock_partitions_by_execution(execution_id, *args, **kwargs):
        if execution_id == 10:
            mock_first_partition = MagicMock()
            mock_first_partition.name = "Partition1"
            mock_second_partition = MagicMock()
            mock_second_partition.name = "Partition2"
            return [
                MagicMock(partition=mock_first_partition, value="val1"),
                MagicMock(partition=mock_second_partition, value="val2"),
            ]
        elif execution_id == 11:
            partition_mock = MagicMock()
            partition_mock.name = "Partition3"
            return [
                MagicMock(partition=partition_mock, value="val3"),
            ]
        else:
            return []

    mock_services["repository"].get_by_execution.side_effect = get_mock_partitions_by_execution

    service.trigger_tables(mock_table.id)

    mock_services["table_service"].find.assert_called_once_with(table_id=1)
    mock_services["table_service"].find_by_dependency.assert_called_once_with(1)
    
    assert mock_services["table_execution_service"].get_latest_execution_with_restrictions.call_count == 2
    
    expected_calls = [
        call(2, {"Partition1": "val1", "Partition2": "val2"}),
        call(3, {"Partition1": "val1", "Partition2": "val2"}),
    ]
    
    assert mock_services["event_bridge_scheduler_service"].register_or_postergate_event.call_count == 2

    expected_calls = [
        call(mock_task_table_1, mock_table_execution, mock_dependency_execution, {"Partition3": "val3"}),
        call(mock_task_table_2, mock_table_execution, None, {}),
    ]

    mock_services["event_bridge_scheduler_service"].register_or_postergate_event.assert_has_calls(expected_calls, any_order=True)

def test_register_partitions_exec_success(service, mock_services):
    """Test that `register_partitions_exec` successfully registers partitions."""
    mock_services["table_service"].find.return_value = Tables(id=1, name="MainTable", partitions=[
        MagicMock(id=1, name="Partition1", is_required=True),
        MagicMock(id=2, name="Partition2", is_required=False),
    ])
    mock_services["table_execution_service"].create_execution.return_value = TableExecution(id=10, table_id=1)

    dto = TablePartitionExecDTO(
        table_id=1,
        partitions=[
            PartitionDTO(partition_id=1, value="val1"),
            PartitionDTO(partition_id=2, value="val2"),
        ],
        source="TestSource"
    )

    result = service.register_partitions_exec(dto)

    # Validate repository save calls
    assert mock_services["repository"].save.call_count == 2
    mock_services["table_service"].find.assert_called_once_with(table_id=1)
    mock_services["table_execution_service"].create_execution.assert_called_once_with(1, "TestSource")
    mock_services["event_bridge_scheduler_service"].register_or_postergate_event.assert_called_once()

    assert result == {"message": "Table partition execution entries registered successfully."}

def test_register_partitions_exec_missing_partition(service, mock_services):
    """Test that `register_partitions_exec` raises error when required partition is missing."""
    mock_services["table_service"].find.return_value = Tables(id=1, name="MainTable", partitions=[
        MagicMock(id=1, name="Partition1", is_required=True),
        MagicMock(id=2, name="Partition2", is_required=False),
    ])

    dto = TablePartitionExecDTO(
        table_id=1,
        partitions=[
            PartitionDTO(partition_id=2, value="val2"), 
        ],
        source="TestSource"
    )

    with pytest.raises(TableInsertError, match="As seguintes partições obrigatórias estão faltando: Partition1"):
        service.register_partitions_exec(dto)

def test_register_partitions_exec_invalid_partition(service, mock_services):
    """Test that `register_partitions_exec` raises error when partition does not belong to the table."""
    mock_services["table_service"].find.return_value = Tables(id=1, name="MainTable", partitions=[
        MagicMock(id=1, name="Partition1", is_required=True),
        MagicMock(id=2, name="Partition2", is_required=False),
    ])

    dto = TablePartitionExecDTO(
        table_id=1,
        partitions=[
            PartitionDTO(partition_id=3, value="val3"),  
        ],
        source="TestSource"
    )

    with pytest.raises(TableInsertError, match="Partição com ID '3' não está associada à tabela 'MainTable'."):
        service.register_partitions_exec(dto)
