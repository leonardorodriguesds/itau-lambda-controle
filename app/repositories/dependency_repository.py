from logging import Logger
from models.dependencies import Dependencies

class DependencyRepository:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger

    def get_by_table_id(self, table_id):
        self.logger.debug(f"[DependencyRepository] Getting dependencies for table [{table_id}]")
        return self.session.query(Dependencies).filter(Dependencies.table_id == table_id).all()

    def save(self, dependency):
        self.logger.debug(f"[DependencyRepository] Saving dependency [{dependency}]")
        self.session.add(dependency)
        self.session.commit()
