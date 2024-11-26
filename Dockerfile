FROM public.ecr.aws/lambda/python:3.9
# Copia o código da Lambda
COPY app/ .
# Configura o comando handler
CMD ["lambda_function.lambda_handler"]
