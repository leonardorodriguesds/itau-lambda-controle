from sqlalchemy import Column, Integer, String, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class TaskTable(Base):
    __tablename__ = 'task_table'
    
    table_id = Column(Integer, ForeignKey('tables.id'), primary_key=True)
    task_executor_id = Column(Integer, ForeignKey('task_executor.id'), primary_key=True)
    alias = Column(String(255), primary_key=True)
    params = Column(JSON)
    
    table = relationship("Tables", back_populates="task_table", foreign_keys=[table_id])
    task_executor = relationship("TaskExecutor", back_populates="task_table", foreign_keys=[task_executor_id])
