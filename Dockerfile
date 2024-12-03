FROM public.ecr.aws/lambda/python:3.10

# Define o diretório de trabalho dentro do container
WORKDIR /var/task

# Copia os arquivos de código da Lambda para o container
COPY app/ .

# Instala as dependências listadas em requirements.txt, se aplicável
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --target .

# Configura o handler da Lambda
CMD ["lambda_function.lambda_handler"]
