from datetime import datetime
import pytest
from botocore.exceptions import ClientError
from unittest.mock import MagicMock, PropertyMock
from src.app.models.table_execution import TableExecution
from src.app.models.task_executor import TaskExecutor
from src.app.models.task_schedule import TaskSchedule
from src.app.models.task_table import TaskTable
from src.app.service.event_bridge_scheduler_service import EventBridgeSchedulerService

@pytest.fixture
def mock_services():
    """Mock all dependencies for EventBridgeSchedulerService."""
    return {
        "logger": MagicMock(),
        "boto_service": MagicMock(),
        "task_schedule_service": MagicMock()
    }

@pytest.fixture
def service(mock_services):
    """Create an instance of EventBridgeSchedulerService with mocked dependencies."""
    service = EventBridgeSchedulerService(**mock_services)
    service.scheduler_client = MagicMock()

    type(service.scheduler_client).exceptions = PropertyMock(
        return_value=MagicMock(Exception=ClientError)
    )
    return service

def test_build_event_payload(service: EventBridgeSchedulerService):
    task_table = MagicMock(spec=TaskTable)
    task_table.id = 1
    task_table.alias = "example_alias"
    task_table.debounce_seconds = 10

    trigger_execution = MagicMock(spec=TableExecution)
    trigger_execution.id = 100
    trigger_execution.source = "source_example"
    trigger_execution.date_time = datetime(2024, 12, 18, 15, 30)

    task_schedule = MagicMock(spec=TaskSchedule)
    task_schedule.id = 200
    task_schedule.schedule_alias = "schedule_example"
    task_schedule.unique_alias = "unique_schedule_alias"

    partitions = {
        "partition1": "value1",
        "partition2": "value2"
    }

    result = service.build_event_payload(task_table, trigger_execution, task_schedule, partitions)

    assert result["httpMethod"] == "POST"
    assert result["path"] == "/trigger"
    assert result["body"]["execution"]["id"] == 100
    assert result["body"]["execution"]["source"] == "source_example"
    assert result["body"]["execution"]["timestamp"] == "2024-12-18T15:30:00"
    assert result["body"]["task_table"]["id"] == 1
    assert result["body"]["task_table"]["alias"] == "example_alias"
    assert result["body"]["task_schedule"]["id"] == 200
    assert result["body"]["task_schedule"]["schedule_alias"] == "schedule_example"
    assert result["body"]["task_schedule"]["unique_alias"] == "unique_schedule_alias"
    assert result["body"]["partitions"] == partitions
    assert result["metadata"]["unique_alias"] == "unique_schedule_alias"
    assert result["metadata"]["debounce_seconds"] == 10
    
def test_dict_to_clean_string(service):
    input_dict = {"key@1": "value#1", "key_2": "value 2", "key-3": "value!3"}
    expected_output = "key1=value1-key2=value2-key3=value3"
    result = service.dict_to_clean_string(input_dict)
    assert result == expected_output

    input_dict = {}
    expected_output = ""
    result = service.dict_to_clean_string(input_dict)
    assert result == expected_output

    input_dict = {"key1": 123, "key2": 456.78}
    expected_output = "key1=123-key2=45678"
    result = service.dict_to_clean_string(input_dict)
    assert result == expected_output

def test_generate_unique_alias(service):
    task_table = MagicMock(spec=TaskTable)
    task_table.table.name = "TestTable"
    task_table.alias = "TestAlias"

    last_execution = MagicMock(spec=TableExecution)
    last_execution.id = 42

    partitions = {"partition1": "value1", "partition2": "value2"}
    expected_alias = "TestTable-TestAlias-42-partition1=value1-partition2=value2"
    result = service.generate_unique_alias(task_table, last_execution, partitions)
    assert result == expected_alias

    partitions = {}
    expected_alias = "TestTable-TestAlias-42-NoPartitions"
    result = service.generate_unique_alias(task_table, last_execution, partitions)
    assert result == expected_alias

    last_execution = None
    partitions = {"partition1": "value1"}
    expected_alias = "TestTable-TestAlias-None-partition1=value1"
    result = service.generate_unique_alias(task_table, last_execution, partitions)
    assert result == expected_alias
    
def test_check_event_exists_success(service):
    service.scheduler_client.list_schedules.return_value = {
        "Schedules": [{"Name": "test_schedule_alias"}]
    }
    schedule_alias = "test_schedule_alias"
    result = service.check_event_exists(schedule_alias)
    assert result is True
    service.scheduler_client.list_schedules.assert_called_once_with(NamePrefix=schedule_alias)

def test_check_event_exists_not_found(service):
    service.scheduler_client.list_schedules.return_value = {"Schedules": []}
    schedule_alias = "test_schedule_alias"
    result = service.check_event_exists(schedule_alias)
    assert result is False
    service.scheduler_client.list_schedules.assert_called_once_with(NamePrefix=schedule_alias)

def test_check_event_exists_resource_not_found_exception(service):
    service.scheduler_client.list_schedules.side_effect = (
        service.scheduler_client.exceptions.ResourceNotFoundException(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}}, 
            "list_schedules"
        )
    )
    schedule_alias = "test_schedule_alias"
    result = service.check_event_exists(schedule_alias)
    assert result is False
    service.scheduler_client.list_schedules.assert_called_once_with(NamePrefix=schedule_alias)
    
def test_register_or_postergate_event(service, mock_services):
    task_table = MagicMock(spec=TaskTable)
    task_table.id = 1
    task_table.alias = "example_alias"
    task_table.debounce_seconds = 60
    task_table.task_executor = MagicMock(spec=TaskExecutor)
    task_table.task_executor.identification = "arn:aws:lambda:region:account:function"
    task_table.task_executor.target_role_arn = "arn:aws:iam::account:role/service-role"

    trigger_execution = MagicMock(spec=TableExecution)
    trigger_execution.id = 101
    trigger_execution.source = "source_example"
    trigger_execution.date_time = datetime(2024, 12, 18, 15, 30)

    last_execution = MagicMock(spec=TableExecution)
    last_execution.id = 100

    partitions = {"partition1": "value1"}
    
    unique_alias = "unique_alias"
    
    mock_task_schedule_service = mock_services["task_schedule_service"]
    task_schedule_mock = MagicMock(spec=TaskSchedule)
    task_schedule_mock.id = 1
    task_schedule_mock.unique_alias = unique_alias
    task_schedule_mock.schedule_alias = "schedule_alias"
    mock_task_schedule_service.save.return_value = task_schedule_mock

    service.task_schedule_service.get_by_unique_alias_and_pendent.return_value = None

    service.register_or_postergate_event(task_table, trigger_execution, last_execution, partitions)

    service.task_schedule_service.get_by_unique_alias_and_pendent.assert_called_once()
    service.scheduler_client.create_schedule.assert_called_once()

def test_register_event(service, mock_services):
    task_table = MagicMock(spec=TaskTable)
    task_table.id = 1
    task_table.alias = "example_alias"
    task_table.debounce_seconds = 60
    task_table.task_executor = MagicMock(spec=TaskExecutor)
    task_table.task_executor.identification = "arn:aws:lambda:region:account:function"
    task_table.task_executor.target_role_arn = "arn:aws:iam::account:role/service-role"

    trigger_execution = MagicMock(spec=TableExecution)
    trigger_execution.id = 101
    trigger_execution.source = "source_example"
    trigger_execution.date_time = datetime(2024, 12, 18, 15, 30)

    last_execution = MagicMock(spec=TableExecution)
    last_execution.id = 100

    partitions = {"partition1": "value1"}
    
    unique_alias = "unique_alias"
    
    mock_task_schedule_service = mock_services["task_schedule_service"]
    task_schedule_mock = MagicMock(spec=TaskSchedule)
    task_schedule_mock.id = 1
    task_schedule_mock.unique_alias = unique_alias
    task_schedule_mock.schedule_alias = "schedule_alias"
    mock_task_schedule_service.save.return_value = task_schedule_mock

    service.register_event(None, unique_alias, task_table, trigger_execution, partitions)

    service.scheduler_client.create_schedule.assert_called_once()

def test_postergate_event(service, mock_services):
    task_schedule = MagicMock(spec=TaskSchedule)
    task_schedule.id = 1
    task_schedule.unique_alias = "unique_alias"
    task_schedule.schedule_alias = "schedule_alias"
    task_table_mock = MagicMock(spec=TaskTable)
    task_table_mock.debounce_seconds = 60
    task_table_mock.id = 1
    task_table_mock.alias = "example_alias"
    task_schedule.task_table = task_table_mock
    task_schedule.task_table.task_executor = MagicMock(spec=TaskExecutor)
    task_schedule.task_table.task_executor.identification = "arn:aws:lambda:region:account:function"
    task_schedule.task_table.task_executor.target_role_arn = "arn:aws:iam::account:role/service-role"

    trigger_execution = MagicMock(spec=TableExecution)
    trigger_execution.id = 101
    trigger_execution.source = "source_example"
    trigger_execution.date_time = datetime(2024, 12, 18, 15, 30)
    
    mock_task_schedule_service = mock_services["task_schedule_service"]
    mock_task_schedule_service.save.return_value = task_schedule

    service.postergate_event(task_schedule.unique_alias, task_schedule, trigger_execution, partitions={})

    service.scheduler_client.update_schedule.assert_called_once()

def test_delete_event(service):
    task_schedule = MagicMock(spec=TaskSchedule)
    task_schedule.id = 1
    task_schedule.schedule_alias = "schedule_alias"

    service.delete_event(task_schedule)

    service.scheduler_client.delete_schedule.assert_called_once_with(Name="schedule_alias")
