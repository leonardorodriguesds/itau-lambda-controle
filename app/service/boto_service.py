import os
from boto3 import Session
from logging import Logger
from injector import inject
from typing import Optional


class BotoService:
    @inject
    def __init__(self, logger: Logger, session_provider: Session):
        self.logger = logger
        self.session = session_provider
        self._clients = {}
        self._resources = {}

    def get_client(self, service_name: str, region_name: Optional[str] = None) -> Session.client:
        """
        Retorna um client do Boto3 para o serviço especificado. Reutiliza instâncias existentes para otimizar.
        Inclui suporte para LocalStack ao usar a variável LOCALSTACK_HOST.
        """
        key = (service_name, region_name)
        if key not in self._clients:
            endpoint_url = self._get_localstack_endpoint(service_name)
            self.logger.debug(f"Creating new client for service: {service_name} in region: {region_name}, endpoint: {endpoint_url}")
            self._clients[key] = self.session.client(
                service_name, 
                region_name=region_name,
                endpoint_url=endpoint_url  
            )
        else:
            self.logger.debug(f"Reusing existing client for service: {service_name} in region: {region_name}")
        return self._clients[key]

    def get_resource(self, service_name: str, region_name: Optional[str] = None) -> Session.resource:
        """
        Retorna um resource do Boto3 para o serviço especificado. Reutiliza instâncias existentes para otimizar.
        Inclui suporte para LocalStack ao usar a variável LOCALSTACK_HOST.
        """
        key = (service_name, region_name)
        if key not in self._resources:
            endpoint_url = self._get_localstack_endpoint(service_name)
            self.logger.debug(f"Creating new resource for service: {service_name} in region: {region_name}, endpoint: {endpoint_url}")
            self._resources[key] = self.session.resource(
                service_name, 
                region_name=region_name,
                endpoint_url=endpoint_url  
            )
        else:
            self.logger.debug(f"Reusing existing resource for service: {service_name} in region: {region_name}")
        return self._resources[key]

    def clear_cache(self):
        """
        Limpa o cache de clients e resources.
        """
        self.logger.debug("Clearing cached clients and resources")
        self._clients.clear()
        self._resources.clear()

    def _get_localstack_endpoint(self, service_name: str) -> Optional[str]:
        """
        Retorna o endpoint do LocalStack se a variável LOCALSTACK_HOST estiver configurada.
        """
        LOCALSTACK_HOST = os.getenv("LOCALSTACK_HOST", "localhost")
        if LOCALSTACK_HOST:
            port = 4566  # Porta padrão do LocalStack
            endpoint_url = f"http://{LOCALSTACK_HOST}:{port}"
            self.logger.debug(f"Using LocalStack endpoint for service {service_name}: {endpoint_url}")
            return endpoint_url
        return None
