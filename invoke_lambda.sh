#!/bin/bash

# Verifica se o arquivo JSON foi fornecido como argumento
if [ "$#" -ne 1 ]; then
    echo "Uso: $0 <caminho_para_arquivo_json>"
    exit 1
fi

JSON_FILE="$1"

# Verifica se o arquivo existe
if [ ! -f "$JSON_FILE" ]; then
    echo "Erro: Arquivo JSON '$JSON_FILE' não encontrado."
    exit 1
fi

# Ler o JSON e garantir que o corpo ("body") esteja serializado como string JSON
EVENT=$(jq 'if .body and (type == "object" or type == "array") then .body |= @json else . end' "$JSON_FILE")

if [ -z "$EVENT" ]; then
    echo "Erro: Não foi possível processar o arquivo JSON."
    exit 1
fi

# Endpoint do container Lambda
LAMBDA_ENDPOINT="http://localhost:9000/2015-03-31/functions/function/invocations"

# Invoca o container Lambda
echo "Invocando Lambda no container..."
curl -s -X POST "$LAMBDA_ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "$EVENT" \
    | jq .
