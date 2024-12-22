from sqlalchemy import JSON, Column, Integer, ForeignKey, Enum, DateTime, String
from sqlalchemy.orm import relationship
from datetime import datetime

from src.app.config.constants import STATIC_APPROVE_STATUS_APPROVED, STATIC_APPROVE_STATUS_PENDING, STATIC_APPROVE_STATUS_REJECTED
from .base import AbstractBase

class ApprovalStatus(AbstractBase):
    __tablename__ = 'approval_status'
    
    id = Column(Integer, primary_key=True)
    task_schedule_id = Column(Integer, ForeignKey('task_schedule.id'), nullable=False)
    status = Column(Enum(STATIC_APPROVE_STATUS_PENDING, STATIC_APPROVE_STATUS_APPROVED, STATIC_APPROVE_STATUS_REJECTED, name="approval_status_enum"), default=STATIC_APPROVE_STATUS_PENDING)
    requested_at = Column(DateTime, default=datetime.utcnow)
    reviewed_at = Column(DateTime, nullable=True)
    approver_name = Column(String(255), nullable=True) 
    
    task_schedule = relationship("TaskSchedule", back_populates="approval_status", foreign_keys=[task_schedule_id])
