from models.task_executor import TaskExecutor
from repositories.task_executor_repository import TaskExecutorRepository

class TaskExecutorService:
    def __init__(self, session):
        self.task_executor_repo = TaskExecutorRepository(session)

    def ensure_task_executor(self, alias: str, description: str = "") -> TaskExecutor:
        executor = self.task_executor_repo.get_by_alias(alias)
        if not executor:
            executor = TaskExecutor(alias=alias, description=description)
            self.task_executor_repo.save(executor)
        return executor
