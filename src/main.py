from pydantic import BaseModel, ValidationError
from typing import List, Optional, Union
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define os modelos de dados para cada ação específica
class TabelaExecutadaModel(BaseModel):
    tabela: str

class ReexecutarFluxoModel(BaseModel):
    fluxo_id: str

class ReexecutarFluxoComAgrupamentosModel(BaseModel):
    fluxo_id: str
    agrupamentos: List[str]

class ReexecutarFluxoComPayloadModel(BaseModel):
    fluxo_id: str
    novo_payload: dict

class IncluirAtualizarDeletarTabelaModel(BaseModel):
    operacao: str
    tabela: str
    dependencia: Optional[str]

class ConsultarStatusExecucoesModel(BaseModel):
    execucoes_id: List[str]

# Define o modelo principal que vai lidar com todas as ações
class EventModel(BaseModel):
    action: str
    data: Union[
        TabelaExecutadaModel,
        ReexecutarFluxoModel,
        ReexecutarFluxoComAgrupamentosModel,
        ReexecutarFluxoComPayloadModel,
        IncluirAtualizarDeletarTabelaModel,
        ConsultarStatusExecucoesModel,
    ]

def lambda_handler(event, context):
    try:
        # Valida o payload recebido usando o modelo principal
        event_data = EventModel(**event)

        # Escolhe a função correta com base no tipo de ação
        if event_data.action == "tabela_executada":
            return handle_tabela_executada(event_data.data)
        elif event_data.action == "reexecutar_fluxo":
            return handle_reexecutar_fluxo(event_data.data)
        elif event_data.action == "reexecutar_fluxo_com_agrupamentos":
            return handle_reexecutar_fluxo_com_agrupamentos(event_data.data)
        elif event_data.action == "reexecutar_fluxo_com_payload":
            return handle_reexecutar_fluxo_com_payload(event_data.data)
        elif event_data.action == "incluir_atualizar_deletar_tabela":
            return handle_incluir_atualizar_deletar_tabela(event_data.data)
        elif event_data.action == "consultar_status_execucoes":
            return handle_consultar_status_execucoes(event_data.data)
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Ação desconhecida"})
            }
    except ValidationError as e:
        logger.error("Erro de validação", exc_info=True)
        return {
            "statusCode": 422,
            "body": json.dumps({"error": "Erro de validação", "details": e.errors()})
        }
    except Exception as e:
        logger.error("Erro ao processar evento", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Erro interno do servidor"})
        }

# Funções para cada tipo de evento
def handle_tabela_executada(data: TabelaExecutadaModel):
    logger.info(f"Tabela executada: {data.tabela}")
    return {"status": "sucesso", "mensagem": "Tabela executada"}

def handle_reexecutar_fluxo(data: ReexecutarFluxoModel):
    logger.info(f"Reexecutando fluxo: {data.fluxo_id}")
    return {"status": "sucesso", "mensagem": f"Fluxo {data.fluxo_id} reexecutado"}

def handle_reexecutar_fluxo_com_agrupamentos(data: ReexecutarFluxoComAgrupamentosModel):
    logger.info(f"Reexecutando fluxo {data.fluxo_id} com agrupamentos: {data.agrupamentos}")
    return {"status": "sucesso", "mensagem": f"Fluxo {data.fluxo_id} reexecutado com agrupamentos"}

def handle_reexecutar_fluxo_com_payload(data: ReexecutarFluxoComPayloadModel):
    logger.info(f"Reexecutando fluxo {data.fluxo_id} com payload: {data.novo_payload}")
    return {"status": "sucesso", "mensagem": f"Fluxo {data.fluxo_id} reexecutado com novo payload"}

def handle_incluir_atualizar_deletar_tabela(data: IncluirAtualizarDeletarTabelaModel):
    logger.info(f"{data.operacao} na tabela {data.tabela} com dependência: {data.dependencia}")
    return {"status": "sucesso", "mensagem": f"{data.operacao} realizada na tabela {data.tabela}"}

def handle_consultar_status_execucoes(data: ConsultarStatusExecucoesModel):
    logger.info(f"Consultando status das execuções: {data.execucoes_id}")
    return {"status": "sucesso", "mensagem": f"Status consultado para execuções {data.execucoes_id}"}
