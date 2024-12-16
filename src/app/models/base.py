from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AbstractBase(Base):   
    __abstract__ = True
    
    date_deleted = Column(DateTime, nullable=True)
    deleted_by = Column(String(255), nullable=True)
    tenant_id = Column(Integer, nullable=False, default=1)
    
    def dict(self):
        """
        Retorna um dicionário contendo os atributos principais.
        """
        return {key: getattr(self, key) for key in self.__mapper__.columns.keys()}
