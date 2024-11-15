from models.partitions import Partitions

class PartitionRepository:
    def __init__(self, session):
        self.session = session

    def get_by_table_id(self, table_id):
        return self.session.query(Partitions).filter(Partitions.table_id == table_id).all()

    def save(self, partition):
        self.session.add(partition)
        self.session.commit()
