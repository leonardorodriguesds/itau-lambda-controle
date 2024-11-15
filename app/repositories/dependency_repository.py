from models.dependencies import Dependencies

class DependencyRepository:
    def __init__(self, session):
        self.session = session

    def get_by_table_id(self, table_id):
        return self.session.query(Dependencies).filter(Dependencies.table_id == table_id).all()

    def save(self, dependency):
        self.session.add(dependency)
        self.session.commit()
