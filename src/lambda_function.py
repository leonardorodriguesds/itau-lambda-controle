import json
import argparse
import logging
import os
from injector import Injector
from src.app.config.config import AppModule
from src.app.app import LambdaHandler

def lambda_handler(event, context, injected_injector: Injector = None, debug = False):
    """
    Função Lambda principal, que será chamada pela AWS.
    - NÃO usa decorators
    - Recebe um 'injected_injector' opcional (IoC)
    - Se não for informado nenhum injector, cria um default com AppModule
    - Instancia a classe que processa o evento, injetando as dependências
    - Retorna a resposta
    """
    if os.getenv("DEBUG", "false").lower() == "true":
        debug = True
        
    if injected_injector is not None:
        injector = injected_injector
    else:
        injector = Injector([AppModule()])

    logger = injector.get(logging.Logger)
    
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        
    from aws_lambda_powertools.event_handler import ApiGatewayResolver
    app_resolver = ApiGatewayResolver()
    
    my_handler = LambdaHandler(
        injector=injector,
        app_resolver=app_resolver,
    )

    return my_handler.process_event(event, context)

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

    try:
        with open(args.file, "r", encoding="UTF-8") as file:
            event = json.load(file)

        if "body" in event and isinstance(event["body"], dict):
            event["body"] = json.dumps(event["body"])
            
    except FileNotFoundError:
        print(f"Erro: O arquivo '{args.file}' não foi encontrado.")
        return
    except json.JSONDecodeError:
        print("Erro: Formato JSON inválido.")
        return

    response = lambda_handler(event, context=None, debug=(args.verbose))
    print("Resposta da Lambda:")
    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
