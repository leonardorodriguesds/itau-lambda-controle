from sqlalchemy import Column, Integer, ForeignKey, JSON, Enum, DateTime, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import AbstractBase

class ProcessStatus(AbstractBase):
    __tablename__ = 'process_status'
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    partition_set = Column(JSON, nullable=False)
    status = Column(Enum('IDLE', 'RUNNING', 'EXECUTED', 'FAILED', name="status_enum"), nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    execution_start_time = Column(DateTime, default=None)  
    execution_end_time = Column(DateTime, default=None)  
    execution_logs = Column(Text, nullable=True)  
    
    table = relationship("Tables")
