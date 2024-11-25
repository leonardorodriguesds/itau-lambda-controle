from logging import Logger
from sqlalchemy.orm import Session
from repositories.generic_repository import GenericRepository
from models.tables import Tables

class TableRepository(GenericRepository[Tables]):
    def __init__(self, session: Session, logger: Logger):
        super().__init__(session, Tables, logger)
        self.session = session
        self.logger = logger

    def get_by_name(self, name):
        self.logger.debug(f"[{self.__class__.__name__}] request to get table by name [{name}]")
        return self.session.query(Tables).filter(Tables.name == name).first()
    
    def get_by_dependecy(self, dependecy_id):
        self.logger.debug(f"[{self.__class__.__name__}] request to get tables by dependency_id [{dependecy_id}]")
        return self.session.query(Tables).filter(Tables.dependencies.any(id=dependecy_id)).all()
