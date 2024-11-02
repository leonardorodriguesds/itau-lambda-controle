from typing import List, Optional
from pydantic import BaseModel

class PartitionDTO(BaseModel):
    name: str
    type: str
    is_required: bool

class DependencyDTO(BaseModel):
    id: Optional[int] = None
    table_id: int
    dependency_id: Optional[int] = None
    dependency_name: Optional[str] = None
    dependency_description: Optional[str] = None

class TaskDTO(BaseModel):
    task_executor_id: int
    alias: str
    params: Optional[dict] = None

class TableDTO(BaseModel):
    name: str
    description: Optional[str] = None
    requires_approval: bool = False
    created_by: str
    last_modified_by: Optional[str] = None
    partitions: Optional[List[PartitionDTO]] = []
    dependencies: Optional[List[DependencyDTO]] = []
    tasks: Optional[List[TaskDTO]] = []
