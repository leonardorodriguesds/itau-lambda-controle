import json
import logging
import argparse
from pydantic import ValidationError
from models.dto.table_partition_exec_dto import TablePartitionExecDTO
from service.table_partition_exec_service import TablePartitionExecService
from service.database import get_session
from models.dto.table_dto import TableDTO, validate_tables
from exceptions.table_insert_error import TableInsertError
from service.table_service import TableService

logger = logging.getLogger()
logger.setLevel(logging.INFO) 

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")
    session_generator = get_session()
    session = next(session_generator)
    table_service = TableService(session, logger)
    table_partition_exec_service = TablePartitionExecService(session, logger)

    try:
        event_type = event.get("event")
        data = event.get("data")
        user = event.get("user")

        if not event_type:
            raise ValueError("Event type is required")
        if not data:
            raise ValueError("Data is required")

        if event_type in ["add_table", "update_table"]:
            if isinstance(data, list):
                tables = [TableDTO(**item) for item in data]
                validate_tables(tables)
                message = table_service.save_multiple_tables(tables, user)
            else:
                table = TableDTO(**data)
                message = table_service.save_table(table, user)

            status_code = 200

        elif event_type == "register_execution":
            if isinstance(data, list):
                executions_dto = [TablePartitionExecDTO(**item, user=user) for item in data]
                message = table_partition_exec_service.register_multiple_events(executions_dto)
            else:
                execution_dto = TablePartitionExecDTO(**data, user=user)
                message = table_partition_exec_service.register_partitions_exec(execution_dto)

            status_code = 200

        else:
            raise ValueError(f"Invalid event type: {event_type}")

        return {
            "statusCode": status_code,
            "body": json.dumps({"message": message})
        }

    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Validation error: {e}"})
        }
    except TableInsertError as e:
        logger.error(f"Table insert error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Table insert error: {str(e)}"})
        }
    except ValueError as e:
        logger.error(f"Input error: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Input error: {str(e)}"})
        }
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "An unexpected error occurred"})
        }
    finally:
        session_generator.close()


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
    except FileNotFoundError:
        print(f"Erro: O arquivo '{args.file}' não foi encontrado.")
        return
    except json.JSONDecodeError:
        print("Erro: Formato JSON inválido.")
        return

    response = lambda_handler(event, None)
    print("Resposta da Lambda:")
    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
