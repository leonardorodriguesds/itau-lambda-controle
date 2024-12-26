from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import declarative_base
from .base import AbstractBase

Base = declarative_base()

class TableProcessView(AbstractBase):
    __tablename__ = 'table_process_view'
    __table_args__ = {'extend_existing': True}  

    table_name = Column(String, primary_key=True)
    table_description = Column(String)
    partition_set = Column(String)  
    execution_time = Column(DateTime)
    approver_name = Column(String)
    task_executors = Column(String) 

    def __repr__(self):
        return (
            f"<TableProcessView(table_name='{self.table_name}', "
            f"table_description='{self.table_description}', "
            f"partition_set='{self.partition_set}', "
            f"execution_time='{self.execution_time}', "
            f"approver_name='{self.approver_name}', "
            f"task_executors='{self.task_executors}')>"
        )
