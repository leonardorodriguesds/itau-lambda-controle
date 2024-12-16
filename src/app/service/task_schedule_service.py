from logging import Logger
from typing import Any, Dict, List

from injector import inject

from src.app.models.task_schedule import TaskSchedule
from src.app.repositories.task_schedule_repository import TaskScheduleRepository


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
    
    def find(self, id: int) -> TaskSchedule:
        self.logger.debug(f"[{self.__class__.__name__}] Finding task schedule: [{id}]")
        return self.repository.get_by_id(id)
        
    def query(self, **filters) -> List[TaskSchedule]:
        """
        Consulta agendamentos de tarefas com base em filtros dinâmicos.

        :param filters: Filtros passados como argumentos nomeados (key=value).
        :return: Lista de objetos TaskSchedule que atendem aos filtros.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Querying task schedule with filters: {filters}")
        return self.repository.query(**filters)


        