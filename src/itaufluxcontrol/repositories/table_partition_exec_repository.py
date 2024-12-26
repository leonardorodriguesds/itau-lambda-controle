from logging import Logger
from typing import List
from injector import inject
from sqlalchemy.orm import Session
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.repositories.generic_repository import GenericRepository
from src.itaufluxcontrol.models.table_partition_exec import TablePartitionExec

class TablePartitionExecRepository(GenericRepository[TablePartitionExec]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TablePartitionExec, logger)
        self.session = session_provider.get_session()
        self.logger = logger

    def get_latest_by_table_partition(self, table_id: int, partition_id: int) -> TablePartitionExec:
        """
        Retorna o registro mais recente com `tag_latest = True` para uma combinação de table_id e partition_id.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Getting latest by table and partition: [{table_id}] [{partition_id}]")
        return (
            self.session.query(TablePartitionExec)
            .filter(
                TablePartitionExec.table_id == table_id,
                TablePartitionExec.partition_id == partition_id,
                TablePartitionExec.tag_latest == True,
            )
            .first()
        )

    def get_by_table_partition_and_value(self, table_id: int, partition_id: int, value: str) -> TablePartitionExec:
        self.logger.debug(f"[{self.__class__.__name__}] Getting by table, partition and value: [{table_id}] [{partition_id}] [{value}]")
        return self.session.query(TablePartitionExec).filter_by(
            table_id=table_id,
            partition_id=partition_id,
            value=value
        ).first()
        
    def get_by_execution(self, execution_id: int) -> List[TablePartitionExec]:
        self.logger.debug(f"[{self.__class__.__name__}] Getting partitions exec for execution: [{execution_id}]")
        return self.session.query(TablePartitionExec).filter_by(execution_id=execution_id).all()