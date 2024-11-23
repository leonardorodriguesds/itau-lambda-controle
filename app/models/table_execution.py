from datetime import datetime
import uuid
from sqlalchemy import UUID, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from .base import Base

class TableExecution(Base):
    __tablename__ = 'table_execution'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    date_time = Column(DateTime, default=datetime.utcnow, nullable=False)
    source = Column(String(255), nullable=False)
    
    table = relationship("Tables", back_populates="table_executions")
    table_partition_execs = relationship("TablePartitionExec", back_populates="execution")
