from typing import Optional
from pydantic import BaseModel, Field


class TaskExecutorDTO(BaseModel):
    id: Optional[int] = Field(None, description="ID do executor de tarefa.")
    alias: Optional[str] = Field(None, description="Alias do executor de tarefa.")
    method: Optional[str] = Field(None, description="Método de execução.")
    identification: Optional[str] = Field(None, description="Identificação do executor de tarefa.")
    target_role_arn: Optional[str] = Field(None, description="ARN da role de destino.")