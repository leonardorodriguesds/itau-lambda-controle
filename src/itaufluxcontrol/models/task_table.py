from sqlalchemy import Column, Integer, String, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import AbstractBase

class TaskTable(AbstractBase):
    __tablename__ = 'task_table'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey('tables.id'))
    task_executor_id = Column(Integer, ForeignKey('task_executor.id'))
    alias = Column(String(255))
    params = Column(JSON)
    debounce_seconds = Column(Integer, nullable=False, default=10)
    
    table = relationship("Tables", back_populates="task_table", foreign_keys=[table_id])
    task_executor = relationship("TaskExecutor", back_populates="task_table", foreign_keys=[task_executor_id])
    schedules = relationship("TaskSchedule", back_populates="task_table", foreign_keys="[TaskSchedule.task_id]")