from typing import List, Optional
from pydantic import BaseModel, validator

class PartitionDTO(BaseModel):
    name: str
    type: str
    is_required: bool = False
    sync_column: bool = False

class DependencyDTO(BaseModel):
    dependency_id: Optional[int] = None
    dependency_name: Optional[str] = None
    is_required: bool = False

class TaskDTO(BaseModel):
    id: Optional[int] = None
    task_executor_id: Optional[int] = None
    task_executor: Optional[str] = None
    alias: str
    params: Optional[str] = None
    debounce_seconds: Optional[int] = None

class TableDTO(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    requires_approval: bool = False
    created_by: Optional[str] = None
    last_modified_by: Optional[str] = None
    partitions: Optional[List[PartitionDTO]] = []
    dependencies: Optional[List[DependencyDTO]] = []
    tasks: Optional[List[TaskDTO]] = []

def validate_tables(tables: List[TableDTO]) -> List[TableDTO]:
    names = set()
    table_name_map = {table.name: table for table in tables}

    for table in tables:
        if table.name in names:
            raise ValueError(f"Table name '{table.name}' is not unique.")
        names.add(table.name)

        for dependency in table.dependencies:
            if not dependency.dependency_id and not dependency.dependency_name:
                raise ValueError(
                    f"Dependency in table '{table.name}' must have either a 'id' or a 'name'."
                )
            if dependency.dependency_name and dependency.dependency_name not in table_name_map:
                raise ValueError(
                    f"Dependency name '{dependency.dependency_name}' in table '{table.name}' does not reference a valid table."
                )

    def detect_cycle(current: str, visited: set, stack: set):
        visited.add(current)
        stack.add(current)

        for dependency in table_name_map[current].dependencies:
            if dependency.dependency_name in stack:
                raise ValueError(f"Circular dependency detected involving '{current}'.")
            if dependency.dependency_name not in visited and dependency.dependency_name in table_name_map:
                detect_cycle(dependency.dependency_name, visited, stack)

        stack.remove(current)

    visited = set()
    for table_name in names:
        if table_name not in visited:
            detect_cycle(table_name, visited, set())

    return tables
