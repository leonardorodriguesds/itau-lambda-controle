from typing import List, Optional
from pydantic import BaseModel, Field

class PartitionDTO(BaseModel):
    partition_id: Optional[int] = Field(None, description="ID da partição associada. Opcional se `partition_name` for fornecido.")
    partition_name: Optional[str] = Field(None, description="Nome da partição associada. Opcional se `partition_id` for fornecido.")
    value: str = Field(..., max_length=255, description="Valor da partição.")

class TablePartitionExecDTO(BaseModel):
    table_id: Optional[int] = Field(None, description="ID da tabela. Opcional se `table_name` for fornecido.")
    table_name: Optional[str] = Field(None, description="Nome da tabela. Opcional se `table_id` for fornecido.")
    partitions: List[PartitionDTO] = Field(..., description="Lista de partições e valores.")
    user: str = Field(..., max_length=255, description="Usuário que executou.")
    source: str = Field(..., max_length=255, description="Origem da execução.")
    task_schedule_id: Optional[int] = Field(None, description="ID do agendamento da tarefa. Opcional.")