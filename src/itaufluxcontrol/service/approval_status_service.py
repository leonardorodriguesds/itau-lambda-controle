from datetime import datetime
from logging import Logger
from injector import inject

from src.itaufluxcontrol.config.constants import STATIC_APPROVE_STATUS_APPROVED, STATIC_APPROVE_STATUS_REJECTED
from src.itaufluxcontrol.models.approval_status import ApprovalStatus
from src.itaufluxcontrol.repositories.approval_status_repository import ApprovalStatusRepository


class ApprovalStatusService:
    @inject
    def __init__(self, logger: Logger, repository: ApprovalStatusRepository):
        self.logger = logger
        self.repository = repository
        
    def query(self, **filters):
        self.logger.debug(f"[{self.__class__.__name__}] Querying approval status with filters: [{filters}]")
        return self.repository.query(**filters)
        
    def find(self, id: int) -> ApprovalStatus:
        self.logger.debug(f"[{self.__class__.__name__}] Finding approval status: [{id}]")
        return self.repository.get_by_id(id)
    
    def find_by_task_schedule_id(self, task_schedule_id: int) -> ApprovalStatus:
        self.logger.debug(f"[{self.__class__.__name__}] Finding approval status by task_schedule_id: [{task_schedule_id}]")
        return self.repository.get_by_task_schedule_id(task_schedule_id)
        
    def save(self, approval_status: dict) -> ApprovalStatus:
        self.logger.debug(f"[{self.__class__.__name__}] Saving approval status: {approval_status}")
        approval_status = ApprovalStatus(**approval_status)
        approval_status.requested_at = datetime.now()
        return self.repository.save(approval_status)
    
    def approve(self, id: int, user: str) -> ApprovalStatus:
        self.logger.debug(f"[{self.__class__.__name__}] Approving approval status: [{id}]")
        approval_status = self.find(id)
        approval_status.status = STATIC_APPROVE_STATUS_APPROVED
        approval_status.approver_name = user
        approval_status.reviewed_at = datetime.now()
        return self.repository.save(approval_status)
    
    def reject(self, id: int, user: str) -> ApprovalStatus:
        self.logger.debug(f"[{self.__class__.__name__}] Rejecting approval status: [{id}]")
        approval_status = self.find(id)
        approval_status.status = STATIC_APPROVE_STATUS_REJECTED
        approval_status.approver_name = user
        approval_status.reviewed_at = datetime.now()
        return self.repository.save(approval_status)