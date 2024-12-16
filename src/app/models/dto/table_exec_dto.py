from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel

from src.app.models.table_execution import TableExecution
from src.app.models.table_partition_exec import TablePartitionExec


class TableExecDTO(BaseModel):
    id: int
    name: str
    dependencies: Dict[str, Dict[str, Any]]
    source: str
    date: datetime
    execution: int
    
def transform_to_table_exec_dto(execution: TableExecution, dependencies_executions: Dict[str, Dict[str, Any]]) -> TableExecDTO:
    """
    Transforma uma instância de TableExecution e uma lista de TablePartitionExec em um TableExecDTO.
    
    :param execution: Instância de TableExecution.
    :param partition_execs: Lista de TablePartitionExec associadas.
    :return: TableExecDTO.
    """

    # Construir o DTO
    dto = TableExecDTO(
        id=execution.id,
        name=execution.table.name,  
        dependencies=dependencies_executions,
        source=execution.source,
        date=execution.date_time,
        execution=execution.id
    )
    return dto
    