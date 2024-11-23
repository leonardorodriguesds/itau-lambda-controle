from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, BINARY
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import AbstractBase

class TableExecution(AbstractBase):
    __tablename__ = 'table_execution'
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    date_time = Column(DateTime, default=datetime.utcnow, nullable=False)
    source = Column(String(255), nullable=False)
    deletion_date = Column(DateTime, nullable=True)
    
    table = relationship("Tables", back_populates="table_executions")
    table_partition_execs = relationship("TablePartitionExec", back_populates="execution")