from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from config.session_provider import SessionProvider
from repositories.generic_repository import GenericRepository
from models.dependencies import Dependencies

class DependencyRepository(GenericRepository[Dependencies]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), Dependencies, logger)
        self.session = session_provider.get_session()
        self.logger = logger

    def get_by_table_id(self, table_id):
        self.logger.debug(f"[DependencyRepository] Getting dependencies for table [{table_id}]")
        return self.session.query(Dependencies).filter(Dependencies.table_id == table_id).all()
