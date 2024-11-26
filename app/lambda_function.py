import json
import logging
import argparse
import os
import boto3
from pydantic import ValidationError
from aws_lambda_powertools.event_handler import ApiGatewayResolver
from aws_lambda_powertools.utilities.typing import LambdaContext
from service.database import get_session
from service.table_service import TableService
from service.table_partition_exec_service import TablePartitionExecService
from service.event_bridge_scheduler_service import EventBridgeSchedulerService
from models.dto.table_dto import TableDTO, validate_tables
from models.dto.table_partition_exec_dto import TablePartitionExecDTO
from exceptions.table_insert_error import TableInsertError

# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = ApiGatewayResolver()


def get_session_generator():
    """Cria e retorna um gerador de sessão de banco de dados."""
    session_generator = get_session()
    return session_generator, next(session_generator)


@app.get("/health")
def health_check():
    logger.info("Health check route accessed")
    return {"status": "OK"}


@app.post("/add_table")
def add_table():
    session_generator, session = get_session_generator()
    try:
        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        table_service = TableService(session, logger)
        tables = [TableDTO(**item) for item in data]
        validate_tables(tables)
        message = table_service.save_multiple_tables(tables, user)
        return {"message": message}
    except Exception as e:
        logger.exception(f"Error in add_table: {e}")
        return {"message": f"Error: {str(e)}"}
    finally:
        session_generator.close()


@app.post("/update_table")
def update_table():
    session_generator, session = get_session_generator()
    try:
        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        table_service = TableService(session, logger)
        table = TableDTO(**data)
        message = table_service.save_table(table, user)
        return {"message": message}
    except Exception as e:
        logger.exception(f"Error in update_table: {e}")
        return {"message": f"Error: {str(e)}"}
    finally:
        session_generator.close()


@app.post("/register_execution")
def register_execution():
    session_generator, session = get_session_generator()
    try:
        body = app.current_event.json_body
        data = body.get("data")
        user = body.get("user")

        if not data:
            raise ValueError("Data is required")

        table_partition_exec_service = TablePartitionExecService(session, logger)
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
        session_generator.close()


def lambda_handler(event, context: LambdaContext):
    return app.resolve(event, context)


def main():
    parser = argparse.ArgumentParser(
        description="CLI para executar a função lambda_handler com um payload JSON."
    )
    parser.add_argument(
        "-f", "--file",
        type=str,
        required=True,
        help="Caminho para o arquivo JSON de payload para a função Lambda"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Ativa o modo verboso para exibir informações detalhadas"
    )

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    try:
        with open(args.file, "r") as file:
            event = json.load(file)

        if "body" in event and isinstance(event["body"], dict):
            event["body"] = json.dumps(event["body"])

    except FileNotFoundError:
        print(f"Erro: O arquivo '{args.file}' não foi encontrado.")
        return
    except json.JSONDecodeError:
        print("Erro: Formato JSON inválido.")
        return

    context = None
    response = lambda_handler(event, context)
    
    print("Resposta da Lambda:")
    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
