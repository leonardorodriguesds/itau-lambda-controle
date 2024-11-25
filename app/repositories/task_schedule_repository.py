from logging import Logger
from sqlalchemy.orm import Session
from config.constants import STATIC_SCHEDULE_PENDENT
from models.task_schedule import TaskSchedule
from repositories.generic_repository import GenericRepository


class TaskScheduleRepository(GenericRepository[TaskSchedule]):
    def __init__(self, session: Session, logger: Logger):
        super().__init__(session, TaskSchedule, logger)
        self.session = session
        self.logger = logger
        
    def get_pendent_schedules(self):
        return self.session.query(TaskSchedule).filter(TaskSchedule.status == STATIC_SCHEDULE_PENDENT).all()