from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from .base import AbstractBase

class Partitions(AbstractBase):
    __tablename__ = 'partitions'
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)
    is_required = Column(Boolean, nullable=False, default=False)
    sync_column = Column(Boolean, nullable=True, default=False)
    
    table = relationship("Tables", back_populates="partitions")    
    table_partition_execs = relationship("TablePartitionExec", back_populates="partition")

