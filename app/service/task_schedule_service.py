from logging import Logger
from typing import Any, Dict

from injector import inject

from models.task_schedule import TaskSchedule
from repositories.task_schedule_repository import TaskScheduleRepository


class TaskScheduleService:
    @inject
    def __init__(self, logger: Logger, repository: TaskScheduleRepository):
        self.logger = logger
        self.repository = repository
        
    def get_pendent_schedules(self):
        return self.repository.get_pendent_schedules()
    
    def get_by_unique_alias_and_pendent(self, unique_alias):
        return self.repository.get_by_unique_alias_and_pendent(unique_alias)
    
    def save(self, task_schedule: Dict[str, Any]):
        task_schedule: TaskSchedule = TaskSchedule(**task_schedule)
        return self.repository.save(task_schedule)
        
        