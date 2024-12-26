from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.repositories.generic_repository import GenericRepository
from src.itaufluxcontrol.models.tables import Tables

class TableRepository(GenericRepository[Tables]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), Tables, logger)
        self.session = session_provider.get_session()
        self.logger = logger

    def get_by_name(self, name):
        self.logger.debug(f"[{self.__class__.__name__}] request to get table by name [{name}]")
        return self.session.query(Tables).filter(Tables.name == name).first()
    
    def get_by_dependecy(self, dependecy_id):
        self.logger.debug(f"[{self.__class__.__name__}] request to get tables by dependency_id [{dependecy_id}]")
        return self.session.query(Tables).filter(Tables.dependencies.any(id=dependecy_id)).all()