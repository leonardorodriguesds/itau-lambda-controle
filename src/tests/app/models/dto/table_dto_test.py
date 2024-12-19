import pytest
from pydantic import ValidationError
from src.app.models.dto.table_dto import PartitionDTO, DependencyDTO, TaskDTO, TableDTO, validate_tables

def test_validate_tables():
    partitions = [PartitionDTO(name="partition1", type="string")]
    dependencies = [DependencyDTO(dependency_name="table1")]
    tasks = [
        TaskDTO(id=1, task_executor_id=101, alias="task1", debounce_seconds=30)
    ]

    table1 = TableDTO(
        id=1,
        name="table1",
        description="Test table 1",
        requires_approval=True,
        created_by="user1",
        partitions=partitions,
        dependencies=[],
        tasks=tasks
    )

    table2 = TableDTO(
        id=2,
        name="table2",
        description="Test table 2",
        requires_approval=False,
        created_by="user2",
        dependencies=[DependencyDTO(dependency_name="table1")],
        tasks=tasks
    )

    validate_tables([table1, table2])

    with pytest.raises(ValueError):
        validate_tables([table1, table1])

    table2.dependencies = [DependencyDTO(dependency_name="table3")]
    with pytest.raises(ValueError):
        validate_tables([table1, table2])

    table1.dependencies = [DependencyDTO(dependency_name="table2")]
    table2.dependencies = [DependencyDTO(dependency_name="table1")]
    with pytest.raises(ValueError):
        validate_tables([table1, table2])
