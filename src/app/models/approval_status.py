from sqlalchemy import Column, Integer, ForeignKey, Enum, DateTime, String
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import AbstractBase

class ApprovalStatus(AbstractBase):
    __tablename__ = 'approval_status'
    
    id = Column(Integer, primary_key=True)
    task_table_id = Column(Integer, ForeignKey('task_table.id'), nullable=False)
    status = Column(Enum('PENDING', 'APPROVED', 'REJECTED', name="approval_status_enum"), default='PENDING')
    requested_at = Column(DateTime, default=datetime.utcnow)
    reviewed_at = Column(DateTime, nullable=True)
    approver_name = Column(String(255), nullable=True) 
    
    task_table = relationship("TaskTable", back_populates="approval_status")
