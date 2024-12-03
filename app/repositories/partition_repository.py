from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from provider.session_provider import SessionProvider
from repositories.generic_repository import GenericRepository
from models.partitions import Partitions

class PartitionRepository(GenericRepository[Partitions]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), Partitions, logger)
        self.session = session_provider.get_session()
        self.logger = logger

    def get_by_table_id(self, table_id):
        self.logger.debug(f"[{self.__class__.__name__}] Getting partitions for table [{table_id}]")
        return self.session.query(Partitions).filter(Partitions.table_id == table_id).all()