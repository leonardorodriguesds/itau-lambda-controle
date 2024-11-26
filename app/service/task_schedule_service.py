from logging import Logger

from injector import inject

from repositories.task_schedule_repository import TaskScheduleRepository


class TaskScheduleService:
    @inject
    def __init__(self, logger: Logger, repository: TaskScheduleRepository):
        self.logger = logger
        self.repository = repository
        
        