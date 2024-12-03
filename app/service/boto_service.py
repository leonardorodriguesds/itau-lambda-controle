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
        """
        key = (service_name, region_name)
        if key not in self._clients:
            self.logger.debug(f"Creating new client for service: {service_name} in region: {region_name}")
            self._clients[key] = self.session.client(service_name, region_name=region_name)
        else:
            self.logger.debug(f"Reusing existing client for service: {service_name} in region: {region_name}")
        return self._clients[key]

    def get_resource(self, service_name: str, region_name: Optional[str] = None) -> Session.resource:
        """
        Retorna um resource do Boto3 para o serviço especificado. Reutiliza instâncias existentes para otimizar.
        """
        key = (service_name, region_name)
        if key not in self._resources:
            self.logger.debug(f"Creating new resource for service: {service_name} in region: {region_name}")
            self._resources[key] = self.session.resource(service_name, region_name=region_name)
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
