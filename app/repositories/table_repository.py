from logging import Logger
from models.tables import Tables

class TableRepository:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger

    def get_by_id(self, table_id):
        self.logger.debug(f"[TableRepository] request to get table by id [{table_id}]")
        return self.session.query(Tables).filter(Tables.id == table_id).first()

    def get_by_name(self, name):
        self.logger.debug(f"[TableRepository] request to get table by name [{name}]")
        return self.session.query(Tables).filter(Tables.name == name).first()

    def save(self, table):
        self.logger.debug(f"[TableRepository] request to save table [{table}]")
        self.session.add(table)
        self.session.commit()
        return table
    
    def get_by_dependecy(self, dependecy_id):
        self.logger.debug(f"[TableRepository] request to get tables by dependency_id [{dependecy_id}]")
        return self.session.query(Tables).filter(Tables.dependencies.any(id=dependecy_id)).all()

    def update(self, table):
        self.logger.debug(f"[TableRepository] request to update table [{table}]")
        self.session.commit()
        return table
