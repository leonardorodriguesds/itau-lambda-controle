import os
import boto3
from injector import singleton


class Boto3SessionProvider:
    """Provedor para criar e gerenciar sessões do Boto3."""
    
    @singleton
    def provide_session(self) -> boto3.Session:
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "default_access_key")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "default_secret_key")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        return boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )