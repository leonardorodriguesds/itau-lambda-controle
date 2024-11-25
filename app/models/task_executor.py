from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from .base import AbstractBase

class TaskExecutor(AbstractBase):
    __tablename__ = 'task_executor'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String(255), nullable=False)  
    description = Column(Text, nullable=True)  
    method = Column(String(255), nullable=False)  
    identification = Column(String(1024), nullable=True)  
    target_role_arn = Column(String(255), nullable=True)
    
    task_table = relationship("TaskTable", back_populates="task_executor")
