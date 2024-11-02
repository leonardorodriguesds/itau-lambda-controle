from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from .base import Base

class TaskExecutor(Base):
    __tablename__ = 'task_executor'
    
    id = Column(Integer, primary_key=True)
    alias = Column(String(255), nullable=False)
    description = Column(Text)
    
    tables = relationship("Tables", back_populates="task_executor")
    task_table = relationship("TaskTable", back_populates="task_executor")
