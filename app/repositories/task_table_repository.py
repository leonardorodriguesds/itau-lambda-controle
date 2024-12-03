from logging import Logger
from injector import inject
from provider.session_provider import SessionProvider
from models.task_schedule import TaskSchedule
from repositories.generic_repository import GenericRepository


class TaskTableRepository(GenericRepository[TaskSchedule]):
    @inject
    def __init__(self, session_provider: SessionProvider, logger: Logger):
        super().__init__(session_provider.get_session(), TaskSchedule, logger)
        self.session = session_provider.get_session()
        self.logger = logger