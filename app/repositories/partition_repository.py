from logging import Logger
from models.partitions import Partitions

class PartitionRepository:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger

    def get_by_table_id(self, table_id):
        self.logger.debug(f"[PartitionRepository] Getting partitions for table [{table_id}]")
        return self.session.query(Partitions).filter(Partitions.table_id == table_id).all()

    def save(self, partition):
        self.logger.debug(f"[PartitionRepository] Saving partition [{partition}]")
        self.session.add(partition)
        self.session.commit()