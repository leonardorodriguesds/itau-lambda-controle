from sqlalchemy import Column, Integer, String, JSON, Enum, DateTime
from .base import Base

class TableProcessView(Base):
    __tablename__ = 'table_process_view'
    __table_args__ = {'extend_existing': True}  

    table_name = Column(String, primary_key=True)  
    table_description = Column(String)
    partition_set = Column(JSON)
    process_status = Column(Enum('IDLE', 'RUNNING', 'EXECUTED', 'FAILED', name="status_enum"))
    approval_status = Column(Enum('PENDING', 'APPROVED', 'REJECTED', name="approval_status_enum"))
    approver_name = Column(String)
    execution_time = Column(DateTime)
    approval_reviewed_at = Column(DateTime)

    def __repr__(self):
        return (
            f"<TableProcessView(table_name='{self.table_name}', "
            f"process_status='{self.process_status}', "
            f"approval_status='{self.approval_status}', "
            f"execution_time='{self.execution_time}', "
            f"approval_reviewed_at='{self.approval_reviewed_at}')>"
        )
