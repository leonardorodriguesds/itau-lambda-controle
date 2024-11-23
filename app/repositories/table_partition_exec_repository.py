from sqlalchemy.orm import Session
from models.table_partition_exec import TablePartitionExec

class TablePartitionExecRepository:
    def __init__(self, session: Session):
        self.session = session

    def save(self, exec_entry: TablePartitionExec):
        """
        Salva um registro de execução no banco de dados.
        """
        self.session.add(exec_entry)

    def commit(self):
        """
        Faz o commit das alterações no banco de dados.
        """
        self.session.commit()

    def rollback(self):
        """
        Reverte as alterações no banco de dados.
        """
        self.session.rollback()

    def get_latest_by_table_partition(self, table_id: int, partition_id: int):
        """
        Retorna o registro mais recente com `tag_latest = True` para uma combinação de table_id e partition_id.
        """
        return (
            self.session.query(TablePartitionExec)
            .filter(
                TablePartitionExec.table_id == table_id,
                TablePartitionExec.partition_id == partition_id,
                TablePartitionExec.tag_latest == True,
            )
            .first()
        )

    def get_by_table_partition_and_value(self, table_id: int, partition_id: int, value: str):
        return self.session.query(TablePartitionExec).filter_by(
            table_id=table_id,
            partition_id=partition_id,
            value=value
        ).first()



