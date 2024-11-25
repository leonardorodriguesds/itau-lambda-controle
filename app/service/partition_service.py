from logging import Logger
from typing import List
from models.partitions import Partitions
from models.dto.table_dto import PartitionDTO
from repositories.partition_repository import PartitionRepository

class PartitionService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger
        self.repository = PartitionRepository(session, logger)

    def save_partitions(self, table_id: int, partitions_dto: List[PartitionDTO]):
        self.logger.debug(f"[{self.__class__.__name__}] Saving partitions for table [{table_id}]")
        existing_partitions = {
            p.name for p in self.repository.get_by_table_id(table_id)
        }
        for partition_data in partitions_dto:
            if partition_data.name not in existing_partitions:
                partition = Partitions(
                    table_id=table_id,
                    name=partition_data.name,
                    type=partition_data.type,
                    is_required=partition_data.is_required
                )
                self.repository.save(partition)
