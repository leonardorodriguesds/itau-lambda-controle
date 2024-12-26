from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base  

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
    
    def json_dict(self):
        """
        Retorna um dicionário contendo os atributos principais.
        Converte objetos datetime para strings no formato ISO 8601.
        """
        result = {}
        for key in self.__mapper__.columns.keys():
            value = getattr(self, key)
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result
