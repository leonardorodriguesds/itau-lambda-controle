from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AbstractBase(Base):   
    __abstract__ = True
    def dict(self):
        """
        Retorna um dicionário contendo os atributos principais.
        """
        return {key: getattr(self, key) for key in self.__mapper__.columns.keys()}
