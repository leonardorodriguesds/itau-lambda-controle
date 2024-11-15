from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base

class TablePartitionExec(Base):
    __tablename__ = 'table_partition_exec'
    
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    partition_id = Column(Integer, ForeignKey('partitions.id'), nullable=False)
    value = Column(String(255), nullable=False)
    execution_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    tag_latest = Column(Boolean, nullable=False, default=False)
    deletion_date = Column(DateTime, nullable=True)
    deleted_by_user = Column(String(255), nullable=True)
    
    __table_args__ = (
        UniqueConstraint('table_id', 'partition_id', 'tag_latest', name='unique_table_partition_latest_tag'),
        {"extend_existing": True},
    )
    
    table = relationship("Tables", back_populates="table_partition_execs")
    partition = relationship("Partitions", back_populates="table_partition_execs")
    
    __mapper_args__ = {
        "primary_key": [table_id, partition_id, value]
    }
