from logging import Logger
from sqlalchemy.orm import Session
from repositories.generic_repository import GenericRepository
from models.partitions import Partitions

class PartitionRepository(GenericRepository[Partitions]):
    def __init__(self, session: Session, logger: Logger):
        super().__init__(session, Partitions, logger)
        self.session = session
        self.logger = logger

    def get_by_table_id(self, table_id):
        self.logger.debug(f"[{self.__class__.__name__}] Getting partitions for table [{table_id}]")
        return self.session.query(Partitions).filter(Partitions.table_id == table_id).all()