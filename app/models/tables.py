from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base

class Tables(Base):
    __tablename__ = 'tables'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    requires_approval = Column(Boolean, default=False)
    
    created_by = Column(String(255), nullable=False)
    last_modified_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    partitions = relationship("Partitions", back_populates="table")
    dependencies = relationship("Dependencies", back_populates="table")
    dependent_tables = relationship("Dependencies", foreign_keys="[Dependencies.dependency_id]", back_populates="dependency_table")
    approval_status = relationship("ApprovalStatus", back_populates="table")
    task_table = relationship("TaskTable", back_populates="table") 
