from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from src.itaufluxcontrol.provider.session_provider import SessionProvider
from src.itaufluxcontrol.config.constants import STATIC_SCHEDULE_PENDENT, STATIC_SCHEDULE_WAITING_APPROVAL
from src.itaufluxcontrol.models.task_schedule import TaskSchedule
from src.itaufluxcontrol.repositories.generic_repository import GenericRepository


class TaskScheduleRepository(GenericRepository[TaskSchedule]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskSchedule, logger)
        self.session = session_provider.get_session()
        self.logger = logger
        
    def get_pendent_schedules(self):
        return self.session.query(TaskSchedule).filter(TaskSchedule.status == STATIC_SCHEDULE_PENDENT).all()
    
    def get_by_unique_alias_and_pendent(self, unique_alias):
        self.logger.debug(f"[{self.__class__.__name__}] Getting task schedule by unique alias: {unique_alias}")
        return self.session.query(TaskSchedule).filter(
            TaskSchedule.unique_alias == unique_alias,
            TaskSchedule.status.in_([STATIC_SCHEDULE_PENDENT, STATIC_SCHEDULE_WAITING_APPROVAL])
        ).first()