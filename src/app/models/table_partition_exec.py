from sqlalchemy import BINARY, UUID, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import AbstractBase

class TablePartitionExec(AbstractBase):
    __tablename__ = 'table_partition_exec'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    partition_id = Column(Integer, ForeignKey('partitions.id'), nullable=False)
    value = Column(String(255), nullable=False)
    execution_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    deletion_date = Column(DateTime, nullable=True)
    deleted_by_user = Column(String(255), nullable=True)
    execution_id = Column(Integer, ForeignKey('table_execution.id'), nullable=False)
    
    table = relationship("Tables", back_populates="table_partition_execs")
    partition = relationship("Partitions", back_populates="table_partition_execs")
    execution = relationship("TableExecution", back_populates="table_partition_execs") 
    
    __unique_constraint__ = ('table_id', 'partition_id', 'value', 'execution_id')
