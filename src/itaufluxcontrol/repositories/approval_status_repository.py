from logging import Logger
from injector import inject
from src.itaufluxcontrol.models.approval_status import ApprovalStatus
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.repositories.generic_repository import GenericRepository

class ApprovalStatusRepository(GenericRepository[ApprovalStatus]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), ApprovalStatus, logger)
        self.session = session_provider.get_session()
        self.logger = logger

    def get_by_task_schedule_id(self, task_schedule_id):
        self.logger.debug(f"[{self.__class__.__name__}] Getting ApprovalStatus by task_schedule_id: {task_schedule_id}")
        return self.session.query(ApprovalStatus).filter(ApprovalStatus.task_schedule_id == task_schedule_id).all()
