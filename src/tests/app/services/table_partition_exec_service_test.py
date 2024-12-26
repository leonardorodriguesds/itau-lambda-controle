import pytest
from unittest.mock import ANY, MagicMock, call, patch
from datetime import datetime
from src.itaufluxcontrol.service.table_partition_exec_service import TablePartitionExecService
from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec
from src.itaufluxcontrol.models.tables import Tables
from src.itaufluxcontrol.models.table_execution import TableExecution
from src.itaufluxcontrol.models.dto.table_partition_exec_dto import TablePartitionExecDTO, PartitionDTO
from src.itaufluxcontrol.exceptions.table_insert_error import TableInsertError

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
    
def test_register_partitions_exec_with_table_id(service, mock_services):
    """Test the register_partitions_exec method of TablePartitionExecService."""
    mock_table = MagicMock(id=1, name="TestTable")
    mock_required_partition = MagicMock(id=1, name="RequiredPartition", is_required=True)
    mock_optional_partition = MagicMock(id=2, name="OptionalPartition", is_required=False)
    mock_table.partitions = [mock_required_partition, mock_optional_partition]
    mock_services["table_service"].find.return_value = mock_table

    dto = TablePartitionExecDTO(
        table_id=1,
        table_name=None,
        source="TestSource",
        user="TestUser",
        partitions=[
            PartitionDTO(partition_id=1, partition_name=None, value="Value1"),
            PartitionDTO(partition_id=2, partition_name=None, value="Value2"),
        ]
    )

    mock_services["repository"].save = MagicMock()
    mock_new_execution = MagicMock(id=100, table_id=1)
    mock_services["table_execution_service"].create_execution.return_value = mock_new_execution

    service.register_partitions_exec(dto)

    mock_services["table_service"].find.assert_has_calls([
        call(table_id=1),
        call(table_id=1),
    ])    
    assert mock_services["table_service"].find.call_count == 2
    mock_services["table_execution_service"].create_execution.assert_called_once_with(1, "TestSource")
    mock_services["repository"].save.assert_called()

def test_register_partitions_exec_missing_required_partition(service, mock_services):
    """Test the register_partitions_exec method when required partitions are missing."""
    mock_table = MagicMock(id=1, name="TestTable")
    mock_required_partition = MagicMock()
    mock_required_partition.is_required = True
    mock_required_partition.name = "RequiredPartition"
    mock_required_partition.id = 1
    mock_table.partitions = [mock_required_partition]
    mock_services["table_service"].find.return_value = mock_table

    dto = TablePartitionExecDTO(
        table_id=1,
        table_name=None,
        source="TestSource",
        user="TestUser",
        partitions=[]
    )

    with pytest.raises(TableInsertError) as excinfo:
        service.register_partitions_exec(dto)
        
    assert "RequiredPartition" in str(excinfo.value)

def test_register_partitions_exec_invalid_partition(service, mock_services):
    """Test the register_partitions_exec method with an invalid partition."""
    mock_table = MagicMock(id=1, name="TestTable")
    mock_valid_partition = MagicMock()
    mock_valid_partition.id = 1
    mock_valid_partition.name = "ValidPartition"
    mock_valid_partition.is_required = False
    mock_table.partitions = [mock_valid_partition]
    mock_services["table_service"].find.return_value = mock_table

    dto = TablePartitionExecDTO(
        table_id=1,
        table_name=None,
        source="TestSource",
        user="TestUser",
        partitions=[PartitionDTO(partition_id=99, partition_name=None, value="Value99")]
    )

    with pytest.raises(TableInsertError) as excinfo:
        service.register_partitions_exec(dto)

    assert "não está associada à tabela" in str(excinfo.value)
