from typing import Optional
from pydantic import BaseModel, Field


class TriggerProcess(BaseModel):
    table_id: Optional[int] = Field(None, description="ID da tabela. Opcional se `table_name` for fornecido.")
    table_name: Optional[str] = Field(None, description="Nome da tabela. Opcional se `table_id` for fornecido.")
    task_id: Optional[int] = Field(None, description="ID da tarefa. Opcional se `task_name` for fornecido.")
    task_name: Optional[str] = Field(None, description="Nome da tarefa. Opcional se `task_id` for fornecido.")
    params: Optional[dict] = Field(None, description="Parâmetros da tarefa.")