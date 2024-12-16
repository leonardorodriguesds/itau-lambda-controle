import json
import argparse
import logging
import time
from aws_lambda_powertools.utilities.typing import LambdaContext
from service.cloud_watch_service import CloudWatchService
from routes import app, inject_dependencies

@inject_dependencies
def lambda_handler(event, context: LambdaContext, cloudwatch_service: CloudWatchService):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    start_time = time.time()
    error_count = 0
    route_called = event.get('path', 'unknown')

    try:
        logger.info(f"Processing event on route: {route_called}")

        response = app.resolve(event, context)

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        response = {"message": "Error processing event", "error": str(e)}
        error_count += 1

    finally:
        total_execution_time = time.time() - start_time

        cloudwatch_service.add_metric(name="ExecutionTime", value=total_execution_time, unit="Milliseconds")
        cloudwatch_service.add_metric(name="ErrorCount", value=error_count, unit="Count")
        cloudwatch_service.add_metric(name="RouteCalled", value=1, unit="Count")  

        cloudwatch_service.flush_metrics()

    return response

@inject_dependencies
def main(logger: logging.Logger, cloudwatch_service: CloudWatchService):
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
    response = lambda_handler(event, context, cloudwatch_service)

    print("Resposta da Lambda:")
    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
