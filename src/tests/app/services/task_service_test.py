from unittest import mock
import pytest
from unittest.mock import MagicMock, create_autospec
from datetime import datetime
from src.itaufluxcontrol.service.task_service import TaskService
from src.itaufluxcontrol.models.tables import Tables
from src.itaufluxcontrol.models.task_table import TaskTable
from src.itaufluxcontrol.models.table_execution import TableExecution
from src.itaufluxcontrol.models.task_schedule import TaskSchedule

@pytest.fixture
def task_service():
    logger = MagicMock()
    table_execution_service = MagicMock()
    table_service = MagicMock()
    table_partition_exec_service = MagicMock()
    cloudwatch_service = MagicMock()
    task_table_service = MagicMock()
    boto_service = MagicMock()
    task_schedule_service = MagicMock()
    event_bridge_scheduler_service = MagicMock()

    return TaskService(
        logger,
        table_execution_service,
        table_service,
        table_partition_exec_service,
        cloudwatch_service,
        task_table_service,
        boto_service,
        task_schedule_service,
        event_bridge_scheduler_service
    )

def test_trigger_tables_success(task_service):
    task_schedule_id = 1
    task_table_id = 2
    dependency_execution_id = 3

    task_schedule = TaskSchedule(id=task_schedule_id, status="pending", unique_alias="alias")
    task_service.task_schedule_service.query.return_value = [task_schedule]

    task_table = TaskTable(id=task_table_id, params={}, task_executor=MagicMock(method="lambda_process"))
    task_service.task_table_service.find.return_value = task_table

    table = Tables(name="test_table")
    task_table.table = table

    dependency_execution = TableExecution(id=dependency_execution_id)
    task_service.table_execution_service.find.return_value = dependency_execution

    task_service.table_partition_exec_service.get_by_execution.return_value = []
    task_service._resolve_dependencies = MagicMock(return_value={})
    task_service.process = MagicMock()

    task_service.trigger_tables(task_schedule_id, task_table_id, dependency_execution_id)

    task_service.task_schedule_service.query.assert_called_once_with(id=task_schedule_id, status="pending")
    task_service.task_table_service.find.assert_called_once_with(task_id=task_table_id)
    task_service.table_execution_service.find.assert_called_once_with(id=dependency_execution_id)
    task_service._resolve_dependencies.assert_called_once()
    task_service.process.assert_called_once()

def test_trigger_tables_no_schedule(task_service):
    task_service.task_schedule_service.query.return_value = []

    task_service.trigger_tables(1, 2, 3)

    task_service.logger.warning.assert_called_once_with(
        "[TaskService] Task Schedule não encontrado ou já processado."
    )

def test_trigger_tables_dependency_not_resolved(task_service):
    task_schedule = TaskSchedule(id=1, status="pending", unique_alias="alias")
    task_service.task_schedule_service.query.return_value = [task_schedule]

    task_table = TaskTable(id=2, params={}, task_executor=MagicMock(method="lambda_process"))
    task_service.task_table_service.find.return_value = task_table

    table = Tables(name="test_table")
    task_table.table = table

    dependency_execution = TableExecution(id=3)
    task_service.table_execution_service.find.return_value = dependency_execution

    task_service.table_partition_exec_service.get_by_execution.return_value = []
    task_service._resolve_dependencies = MagicMock(return_value=None)

    task_service.trigger_tables(1, 2, 3)
    task_service.logger.warning.assert_called_once_with(
        "[TaskService][test_table] Dependências não resolvidas. Execução cancelada."
    )

def test_process_with_lambda(task_service):
    task_schedule = TaskSchedule(id=1, unique_alias="alias")
    task_table = MagicMock(spec=TaskTable)
    task_table.params = {}
    task_table.task_executor = MagicMock(method="lambda_process")
    task_table.table = MagicMock(spec=Tables)
    task_table.table.name = "test_table"
    
    execution = MagicMock(spec=TableExecution)
    execution.id = 1
    execution.date_time = datetime.now()
    execution.status = "pending"
    execution.table = task_table.table
    execution.source = "source"
    dependencies_partitions = {}

    task_service._interpolate_payload = MagicMock(return_value={"key": "value"})
    task_service.boto_service.get_client.return_value.invoke.return_value = {"StatusCode": 200}

    task_service.process(task_schedule, task_table, execution, dependencies_partitions)

    task_service._interpolate_payload.assert_called_once()
    task_service.boto_service.get_client.assert_called_once_with("lambda")
    task_service.logger.info.assert_called()

def test_process_with_stepfunctions(task_service):
    task_schedule = TaskSchedule(id=1, unique_alias="alias")
    task_table = MagicMock(spec=TaskTable)
    task_table.params = {}
    task_table.task_executor = MagicMock(method="stepfunction_process")
    task_table.table = MagicMock(spec=Tables)
    task_table.table.name = "test_table"
    task_table.table.id = 1

    execution = MagicMock(spec=TableExecution)
    execution.id = 1
    execution.date_time = datetime.now()
    execution.status = "pending"
    execution.table = task_table.table
    execution.source = "source"
    execution.table_id = task_table.table.id
    dependencies_partitions = {}

    task_service._interpolate_payload = MagicMock(return_value={"key": "value"})
    task_service.boto_service.get_client.return_value.start_execution.return_value = {"ExecutionArn": "arn:aws:states:..."}

    task_service.process(task_schedule, task_table, execution, dependencies_partitions)

    task_service._interpolate_payload.assert_called_once()
    task_service.boto_service.get_client.assert_called_once_with("stepfunctions")
    task_service.logger.info.assert_called()

def test_process_with_sqs(task_service):
    task_schedule = TaskSchedule(id=1, unique_alias="alias")
    task_table = MagicMock(spec=TaskTable)
    task_table.params = {}
    task_table.task_executor = MagicMock(method="sqs_process")
    task_table.table = MagicMock(spec=Tables)
    task_table.table.name = "test_table"

    execution = MagicMock(spec=TableExecution)
    execution.id = 1
    execution.date_time = datetime.now()
    execution.status = "pending"
    execution.table = task_table.table
    execution.source = "source"
    dependencies_partitions = {}

    task_service._interpolate_payload = MagicMock(return_value={"key": "value"})
    task_service.boto_service.get_client.return_value.send_message.return_value = {"MessageId": "msg-id"}

    task_service.process(task_schedule, task_table, execution, dependencies_partitions)

    task_service._interpolate_payload.assert_called_once()
    task_service.boto_service.get_client.assert_called_once_with("sqs")
    task_service.logger.info.assert_called()

def test_process_with_glue(task_service):
    task_schedule = TaskSchedule(id=1, unique_alias="alias")
    task_table = MagicMock(spec=TaskTable)
    task_table.params = {}
    task_table.task_executor = MagicMock(method="glue_process")
    task_table.table = MagicMock(spec=Tables)
    task_table.table.name = "test_table"

    execution = MagicMock(spec=TableExecution)
    execution.id = 1
    execution.date_time = datetime.now()
    execution.status = "pending"
    execution.table = task_table.table
    execution.source = "source"
    dependencies_partitions = {}

    task_service._interpolate_payload = MagicMock(return_value={"key": "value"})
    task_service.boto_service.get_client.return_value.start_job_run.return_value = {"JobRunId": "job-id"}

    task_service.process(task_schedule, task_table, execution, dependencies_partitions)

    task_service._interpolate_payload.assert_called_once()
    task_service.boto_service.get_client.assert_called_once_with("glue")
    task_service.logger.info.assert_called()

def test_process_with_eventbridge(task_service):
    task_schedule = TaskSchedule(id=1, unique_alias="alias")
    task_table = MagicMock(spec=TaskTable)
    task_table.params = {}
    task_table.task_executor = MagicMock(method="eventbridge_process")
    task_table.table = MagicMock(spec=Tables)
    task_table.table.name = "test_table"

    execution = MagicMock(spec=TableExecution)
    execution.id = 1
    execution.date_time = datetime.now()
    execution.status = "pending"
    execution.table = task_table.table
    execution.source = "source"
    dependencies_partitions = {}

    task_service._interpolate_payload = MagicMock(return_value={"key": "value"})
    task_service.boto_service.get_client.return_value.put_events.return_value = {"Entries": [{"EventId": "event-id"}]}

    task_service.process(task_schedule, task_table, execution, dependencies_partitions)

    task_service._interpolate_payload.assert_called_once()
    task_service.boto_service.get_client.assert_called_once_with("events")
    task_service.logger.info.assert_called()

@mock.patch("requests.post")
def test_process_with_api(mock_request_post, task_service):
    task_schedule = TaskSchedule(id=1, unique_alias="alias")
    task_table = MagicMock(spec=TaskTable)
    task_table.params = {}
    task_table.task_executor = MagicMock()
    task_table.task_executor.method = "api_process"
    task_table.task_executor.identification = "http://test.com"
    task_table.table = MagicMock(spec=Tables)
    task_table.table.name = "test_table"

    execution = MagicMock(spec=TableExecution)
    execution.id = 1
    execution.date_time = datetime.now()
    execution.status = "pending"
    execution.table = task_table.table
    execution.source = "source"
    dependencies_partitions = {}

    task_service._interpolate_payload = MagicMock(return_value={"key": "value"})
    mock_request_post.return_value.status_code = 200

    task_service.process(task_schedule, task_table, execution, dependencies_partitions)

    task_service._interpolate_payload.assert_called_once()
    task_service.logger.info.assert_called()
