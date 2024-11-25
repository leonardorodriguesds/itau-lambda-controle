from datetime import datetime
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from models.base import AbstractBase

class TaskSchedule(AbstractBase):
    __tablename__ = 'task_schedule'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('task_table.id'), nullable=False)
    last_event_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    scheduled_execution_time = Column(DateTime, nullable=True)
    status = Column(String(50), default="pending")  
    executed = Column(Boolean, default=False)  
    event_bridge_id = Column(String(255), nullable=False)

    task_table = relationship("TaskTable", back_populates="schedules", foreign_keys=[task_id])