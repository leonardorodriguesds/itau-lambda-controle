from logging import Logger
from sqlalchemy.orm import Session
from repositories.generic_repository import GenericRepository
from models.dependencies import Dependencies

class DependencyRepository(GenericRepository[Dependencies]):
    def __init__(self, session: Session, logger: Logger):
        super().__init__(session, Dependencies, logger)
        self.session = session
        self.logger = logger

    def get_by_table_id(self, table_id):
        self.logger.debug(f"[DependencyRepository] Getting dependencies for table [{table_id}]")
        return self.session.query(Dependencies).filter(Dependencies.table_id == table_id).all()
