from datetime import datetime
from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from src.itaufluxcontrol.config.constants import STATIC_SCHEDULE_COMPLETED, STATIC_SCHEDULE_FAILED, STATIC_SCHEDULE_IN_PROGRESS, STATIC_SCHEDULE_PENDENT, STATIC_SCHEDULE_WAITING_APPROVAL

from .base import AbstractBase

class TaskSchedule(AbstractBase):
    __tablename__ = 'task_schedule'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('task_table.id'), nullable=False)
    last_event_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    scheduled_execution_time = Column(DateTime, nullable=True)  
    status = Column(Enum(STATIC_SCHEDULE_PENDENT, STATIC_SCHEDULE_IN_PROGRESS, STATIC_SCHEDULE_COMPLETED, STATIC_SCHEDULE_FAILED, STATIC_SCHEDULE_WAITING_APPROVAL, name="task_schedule_status_enum"), default=STATIC_SCHEDULE_PENDENT)
    executed = Column(Boolean, default=False)  
    table_execution_id = Column(Integer, ForeignKey('table_execution.id'), nullable=False)
    unique_alias = Column(String(350), nullable=False)
    schedule_alias = Column(String(64), nullable=True)
    execution_arn = Column(String(350), nullable=True)
    result_execution_id = Column(Integer, ForeignKey('table_execution.id'), nullable=True)
    error_message = Column(String(350), nullable=True)
    partitions = Column(JSON, nullable=True)
    
    task_table = relationship("TaskTable", back_populates="schedules", foreign_keys=[task_id])
    table_execution = relationship("TableExecution", back_populates="schedules", foreign_keys=[table_execution_id])
    result_execution = relationship("TableExecution", foreign_keys=[result_execution_id])
    approval_status = relationship("ApprovalStatus", back_populates="task_schedule", foreign_keys="ApprovalStatus.task_schedule_id")