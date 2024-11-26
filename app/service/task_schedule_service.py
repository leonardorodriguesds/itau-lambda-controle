from logging import Logger

from repositories.task_schedule_repository import TaskScheduleRepository


class TaskScheduleService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger
        self.repository = TaskScheduleRepository(session, logger)
        
        