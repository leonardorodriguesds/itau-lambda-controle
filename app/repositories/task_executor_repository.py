from models.task_executor import TaskExecutor

class TaskExecutorRepository:
    def __init__(self, session):
        self.session = session

    def get_by_id(self, executor_id):
        return self.session.query(TaskExecutor).filter(TaskExecutor.id == executor_id).first()

    def save(self, executor):
        self.session.add(executor)
        self.session.commit()
        return executor
