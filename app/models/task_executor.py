from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from .base import AbstractBase

class TaskExecutor(AbstractBase):
    __tablename__ = 'task_executor'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = Column(String(255), nullable=False)  
    description = Column(Text, nullable=True)  
    method = Column(String(255), nullable=False)  
    sqs_arn = Column(String(1024), nullable=True)  
    stf_arn = Column(String(1024), nullable=True) 
    api_uri = Column(String(1024), nullable=True)  
    kafka_topic = Column(String(255), nullable=True)  
    
    task_table = relationship("TaskTable", back_populates="task_executor")
