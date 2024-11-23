from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel

from models.table_execution import TableExecution
from models.table_partition_exec import TablePartitionExec


class TableExecDTO(BaseModel):
    id: int
    name: str
    partitions: Dict[str, Any]
    source: str
    date: datetime
    execution: int
    
def transform_to_table_exec_dto(execution: TableExecution, partition_execs: List[TablePartitionExec]) -> TableExecDTO:
    """
    Transforma uma instância de TableExecution e uma lista de TablePartitionExec em um TableExecDTO.
    
    :param execution: Instância de TableExecution.
    :param partition_execs: Lista de TablePartitionExec associadas.
    :return: TableExecDTO.
    """
    partitions_dict = {exec.partition.name: exec.value for exec in partition_execs}

    # Construir o DTO
    dto = TableExecDTO(
        id=execution.id,
        name=execution.table.name,  
        partitions=partitions_dict,
        source=execution.source,
        date=execution.date_time,
        execution=execution.id
    )
    return dto
    