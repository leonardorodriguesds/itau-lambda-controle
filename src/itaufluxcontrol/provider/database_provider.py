import json
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from injector import inject
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from src.itaufluxcontrol.service.boto_service import BotoService

class DatabaseProvider:
    @inject
    def __init__(self, boto_service: BotoService):
        self.boto_service = boto_service
        self._configure_database()

    def _get_secret(self, secret_name):
        try:
            client = self.boto_service.get_client("secretsmanager")
            response = client.get_secret_value(SecretId=secret_name)
            return response["SecretString"]
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise Exception(f"Erro ao obter credenciais do Secrets Manager: {str(e)}")
        except Exception as e:
            raise Exception(f"Erro inesperado ao acessar o Secrets Manager: {str(e)}")

    def _configure_database(self):
        secret_name = os.getenv("DB_SECRET_NAME", "default_secret")
        region_name = os.getenv("AWS_REGION", "us-east-1")

        secret = self._get_secret(secret_name)
        credentials = json.loads(secret)

        db_user = credentials.get("username", "user")
        db_password = credentials.get("password", "password")
        db_host = credentials.get("host", "localhost")
        db_port = credentials.get("port", "3306")
        db_name = credentials.get("dbname", "lambdacontrole")

        database_url = (
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            "?charset=utf8mb4"
        )

        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={"charset": "utf8mb4"},
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def set_charset(self, db):
        db.execute(text("SET NAMES utf8mb4;"))
        db.execute(text("SET CHARACTER SET utf8mb4;"))
        db.execute(text("SET character_set_connection=utf8mb4;"))

    def get_session(self):
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()
