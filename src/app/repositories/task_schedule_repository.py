from logging import Logger
from injector import inject
from sqlalchemy.orm import Session
from src.app.provider.session_provider import SessionProvider
from src.app.config.constants import STATIC_SCHEDULE_PENDENT
from src.app.models.task_schedule import TaskSchedule
from src.app.repositories.generic_repository import GenericRepository


class TaskScheduleRepository(GenericRepository[TaskSchedule]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskSchedule, logger)
        self.session = session_provider.get_session()
        self.logger = logger
        
    def get_pendent_schedules(self):
        return self.session.query(TaskSchedule).filter(TaskSchedule.status == STATIC_SCHEDULE_PENDENT).all()
    
    def get_by_unique_alias_and_pendent(self, unique_alias):
        return self.session.query(TaskSchedule).filter(TaskSchedule.unique_alias == unique_alias, TaskSchedule.status == STATIC_SCHEDULE_PENDENT).first()