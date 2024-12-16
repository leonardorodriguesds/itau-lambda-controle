#!/bin/bash

# Verifica se o nome do scheduler foi fornecido
if [ -z "$1" ]; then
  echo "Usage: $0 <scheduler_name>"
  exit 1
fi

# Variáveis
SCHEDULER_NAME=$1
TMP_DIR="/tmp/events"
JSON_FILE="$TMP_DIR/${SCHEDULER_NAME}_event.json"

# Criar o diretório temporário, caso não exista
mkdir -p $TMP_DIR

# Recupera o target input do EventBridge Scheduler usando awslocal
TARGET_INPUT=$(awslocal scheduler get-schedule --name $SCHEDULER_NAME | jq -r '.Target.Input')

# Verifica se o comando foi bem-sucedido e o TARGET_INPUT foi recuperado
if [ $? -ne 0 ] || [ -z "$TARGET_INPUT" ]; then
  echo "Error: Could not retrieve input for scheduler $SCHEDULER_NAME."
  exit 1
fi

# Cria o arquivo JSON temporário com o target input
echo "$TARGET_INPUT" > $JSON_FILE
if [ $? -ne 0 ]; then
  echo "Error: Failed to create JSON file."
  exit 1
fi

# Executa o programa Python com o arquivo JSON
python3 app/lambda_function.py -f $JSON_FILE -v
PYTHON_EXIT_CODE=$?

# Remove o arquivo JSON temporário após a execução
rm -f $JSON_FILE

# Verifica o resultado da execução do programa Python
if [ $PYTHON_EXIT_CODE -eq 0 ]; then
  echo "Python program executed successfully."
else
  echo "Error: Python program execution failed."
  exit $PYTHON_EXIT_CODE
fi

echo "Event processing completed successfully."
