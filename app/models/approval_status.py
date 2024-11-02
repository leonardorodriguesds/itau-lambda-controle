from sqlalchemy import Column, Integer, ForeignKey, Enum, DateTime, String
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base

class ApprovalStatus(Base):
    __tablename__ = 'approval_status'
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('tables.id'), nullable=False)
    status = Column(Enum('PENDING', 'APPROVED', 'REJECTED', name="approval_status_enum"), default='PENDING')
    requested_at = Column(DateTime, default=datetime.utcnow)
    reviewed_at = Column(DateTime, nullable=True)
    approver_name = Column(String(255), nullable=True) 
    
    table = relationship("Tables", back_populates="approval_status")
