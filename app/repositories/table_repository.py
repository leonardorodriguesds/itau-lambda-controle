from models.tables import Tables

class TableRepository:
    def __init__(self, session):
        self.session = session

    def get_by_id(self, table_id):
        return self.session.query(Tables).filter(Tables.id == table_id).first()

    def get_by_name(self, name):
        return self.session.query(Tables).filter(Tables.name == name).first()

    def save(self, table):
        self.session.add(table)
        self.session.commit()
        return table

    def update(self, table):
        self.session.commit()
        return table
