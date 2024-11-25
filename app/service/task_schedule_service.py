from apscheduler.schedulers.background import BackgroundScheduler
from logging import Logger

from models.task_schedule import TaskSchedule
from repositories.task_schedule_repository import TaskScheduleRepository


class TaskScheduleService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger
        self.repository = TaskScheduleRepository(session, logger)
        self.scheduler = BackgroundScheduler()
        
    def start_schedule(self):
        self.logger.debug(f"[{self.__class__.__name__}] Starting schedule...")
        self.scheduler.start()
        
        pendent_schedules = self.repository.get_pendent_schedules()
        for schedule in pendent_schedules:
            scheduled_execution_time = schedule.scheduled_execution_time
            self.logger.debug(f"[{self.__class__.__name__}] Scheduling task [{schedule.task_id}] for [{scheduled_execution_time}]")
            self.scheduler.add_job(self.execute_task, 'date', run_date=scheduled_execution_time, args=[schedule])
            
    def execute_task(self, schedule: TaskSchedule):
        self.logger.debug(f"[{self.__class__.__name__}] Executing task [{schedule.task_id}]")
        # Aqui você implementaria a lógica para executar a tarefa
        pass
        
        