from functools import wraps
from logging import Logger
from typing import Callable
from inspect import signature
from aws_lambda_powertools.event_handler import ApiGatewayResolver
from src.app.service.task_service import TaskService
from src.app.config.config import injector
from src.app.models.dto.table_dto import TableDTO, validate_tables
from src.app.models.dto.table_partition_exec_dto import TablePartitionExecDTO
from src.app.service.table_service import TableService
from src.app.service.table_partition_exec_service import TablePartitionExecService
from src.app.provider.session_provider import SessionProvider

app = ApiGatewayResolver()

def inject_dependencies(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_signature = signature(func)
        dependencies = {
            param_name: injector.get(param.annotation)
            for param_name, param in func_signature.parameters.items()
            if param.annotation is not param.empty
        }
        return func(*args, **dependencies, **kwargs)
    return wrapper

def transactional(func: Callable):
    """
    Decorator para gerenciar o ciclo de vida da sessão:
    - Commit no sucesso
    - Rollback e log em caso de erro
    - Fechamento de sessão em todos os casos
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        session_provider = kwargs.get("session_provider")
        if not session_provider:
            raise ValueError("`session_provider` é obrigatório para usar o decorator `@transactional`.")
        try:
            result = func(*args, **kwargs)
            session_provider.commit()
            return result
        except Exception as e:
            session_provider.rollback()
            logger = kwargs.get("logger")
            if logger:
                logger.exception(f"Erro na execução de {func.__name__}: {str(e)}")
            raise
        finally:
            session_provider.close()
    return wrapper

def process_entities(func: Callable):
    """
    Annotation para processar múltiplos itens em `data`.
    Garante commit após cada item processado e rollback em caso de erro.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        session_provider: SessionProvider = kwargs.get('session_provider')
        logger = kwargs.get('logger')

        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        messages = []

        try:
            if isinstance(data, list):
                for item in data:
                    try:
                        
                        kwargs["entity_data"] = item
                        kwargs["user"] = user  
                        message = func(*args, **kwargs)
                        messages.append(message)
                        session_provider.commit()  
                        logger.debug(f"Entity processed successfully: {item}")
                    except Exception as e:
                        session_provider.rollback()
                        logger.error(f"Error processing entity: {item}. Error: {e}")
                        raise
            else:
                kwargs["entity_data"] = data 
                kwargs["user"] = user
                message = func(*args, **kwargs)
                messages.append(message)
                session_provider.commit()  

            return {"message": "All entities processed successfully.", "details": messages}
        except Exception as e:
            session_provider.rollback()
            logger.exception(f"Error processing entities: {e}")
            return {"message": f"Error: {str(e)}"}
        finally:
            session_provider.close()

    return wrapper

@app.get("/health")
@inject_dependencies
def health_check(logger: Logger):
    logger.info("Health check route accessed")
    return {"status": "OK"}

@app.post("/add_table")
@inject_dependencies
@process_entities
def add_table(
    table_service: TableService, 
    entity_data: dict,  
    user: str,  
    session_provider: SessionProvider, 
    logger: Logger
):
    table_dto = TableDTO(**entity_data)
    message = table_service.save_table(table_dto, user)
    return message


@app.post("/update_table")
@inject_dependencies
@transactional
def update_table(table_service: TableService, session_provider: SessionProvider, logger: Logger):
    body = app.current_event.json_body
    data = body.get("data")
    user = body.get("user")

    if not data:
        raise ValueError("Data is required")

    table = TableDTO(**data)
    message = table_service.save_table(table, user)
    return {"message": message}

@app.post("/register_execution")
@inject_dependencies
@process_entities
def register_execution(
    table_partition_exec_service: TablePartitionExecService, 
    entity_data: dict,  
    user: str,  
    session_provider: SessionProvider, 
    logger: Logger
):
    execution_dto = TablePartitionExecDTO(**entity_data, user=user)
    message = table_partition_exec_service.register_partitions_exec(execution_dto)
    return message

@app.post("/trigger")
@inject_dependencies
@transactional
def trigger_event(task_service: TaskService, logger: Logger, session_provider: SessionProvider):
    payload = app.current_event.json_body
    
    if not payload:
        raise ValueError("Payload is required")
    
    if not payload.get("task_table"):
        raise ValueError("Task table is required in payload")
    
    if not payload.get("execution"):
        raise ValueError("Execution is required in payload")
    
    if not payload.get("task_schedule"):
        raise ValueError("Task schedule is required in payload")

    task_table_id = payload["task_table"]["id"]
    dependency_execution_id = payload["execution"]["id"]
    task_schedule_id = payload["task_schedule"]["id"]

    logger.debug(f"Received trigger request for task table ID: {payload}")
    logger.info(f"Processing trigger for task table ID: {task_table_id}")

    task_service.trigger_tables(task_schedule_id=task_schedule_id, task_table_id=task_table_id, dependency_execution_id=dependency_execution_id)

    logger.info(f"Trigger processed successfully for task table ID: {task_table_id}")
    return {"message": "Trigger processed successfully."}