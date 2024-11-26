import json
import argparse
import logging
from aws_lambda_powertools.utilities.typing import LambdaContext
from routes import app, inject_dependencies

def lambda_handler(event, context: LambdaContext):
    return app.resolve(event, context)

@inject_dependencies
def main(logger: logging.Logger):
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
