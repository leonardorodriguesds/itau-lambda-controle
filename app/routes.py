from functools import wraps
from logging import Logger
from typing import Callable
from inspect import signature
from aws_lambda_powertools.event_handler import ApiGatewayResolver
from config.config import injector
from models.dto.table_dto import TableDTO, validate_tables
from models.dto.table_partition_exec_dto import TablePartitionExecDTO
from service.table_service import TableService
from service.table_partition_exec_service import TablePartitionExecService
from provider.session_provider import SessionProvider

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

@app.get("/health")
@inject_dependencies
def health_check(logger: Logger):
    logger.info("Health check route accessed")
    return {"status": "OK"}


@app.post("/add_table")
@inject_dependencies
def add_table(table_service: TableService, session_provider: SessionProvider, logger: Logger):
    try:
        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        tables = [TableDTO(**item) for item in data]
        validate_tables(tables)
        message = table_service.save_multiple_tables(tables, user)
        return {"message": message}
    except Exception as e:
        logger.exception(f"Error in add_table: {e}")
        return {"message": f"Error: {str(e)}"}
    finally:
        session_provider.close()


@app.post("/update_table")
@inject_dependencies
def update_table(table_service: TableService, session_provider: SessionProvider, logger: Logger):
    try:
        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        table = TableDTO(**data)
        message = table_service.save_table(table, user)
        return {"message": message}
    except Exception as e:
        logger.exception(f"Error in update_table: {e}")
        return {"message": f"Error: {str(e)}"}
    finally:
        session_provider.close()


@app.post("/register_execution")
@inject_dependencies
def register_execution(
    table_partition_exec_service: TablePartitionExecService,
    session_provider: SessionProvider, 
    logger: Logger
):
    try:
        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        if isinstance(data, list):
            executions_dto = [TablePartitionExecDTO(**item, user=user) for item in data]
            message = table_partition_exec_service.register_multiple_events(executions_dto)
        else:
            execution_dto = TablePartitionExecDTO(**data, user=user)
            message = table_partition_exec_service.register_partitions_exec(execution_dto)

        return {"message": message}
    except Exception as e:
        logger.exception(f"Error in register_execution: {e}")
        return {"message": f"Error: {str(e)}"}
    finally:
        session_provider.close()