from datetime import datetime, timedelta
from logging import Logger
import os
from botocore.session import Session
import boto3

from models.table_execution import TableExecution
from models.task_executor import TaskExecutor
from models.task_table import TaskTable
from models.task_schedule import TaskSchedule

import boto3
from datetime import datetime, timedelta
from logging import Logger

class EventBridgeSchedulerService:
    def __init__(self, session, logger: Logger, boto_session: Session):
        self.session = session
        self.logger = logger
        self.scheduler_client = boto_session.client(
            'events',
            endpoint_url=os.getenv("EVENTBRIDGE_ENDPOINT_URL"),
        )

    def register_event(self, task_table: TaskTable, table_execution: TableExecution, table_partitions_serialized: str):
        self.logger.debug(f"[{self.__class__.__name__}] Registering event for task [{task_table.id}]: [{task_table.alias}]")
        
        task: TaskExecutor = task_table.task_executor
        unique_alias = f"{task_table.alias}-{table_partitions_serialized}-trigger"
        
        schedule_execution_time = datetime.now() + timedelta(seconds=task_table.debbounce_seconds)
        schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")

        payload = {
            "event": "trigger",
            "body": {
                "execution": table_execution.id
            }
        }

        response = self.scheduler_client.create_schedule(
            Name=unique_alias,
            ScheduleExpression=schedule_expression,
            FlexibleTimeWindow={'Mode': 'OFF'},
            Target={
                'Arn': task.identification,  
                'Input': str(payload),
                'RoleArn': task.target_role_arn  
            }
        )

        event_id = response.get('ScheduleArn', 'unknown')
        self.logger.debug(f"[{self.__class__.__name__}] Event registered for task [{task_table.id}] with EventBridge ID [{event_id}]")
        return unique_alias

    def postergate_event(self, task_schedule: TaskSchedule):
        self.logger.debug(f"[{self.__class__.__name__}] Postergating event for task [{task_schedule.task_id}]: [{task_schedule.task_table.alias}]")
        
        task: TaskExecutor = task_schedule.task_table.task_executor
        if task_schedule.unique_alias:
            try:
                schedule_execution_time = datetime.now() + timedelta(seconds=task_schedule.task_table.debounce_seconds)
                schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")
                
                payload = {
                    "event": "trigger",
                    "body": {
                        "execution": task_schedule.table_execution.id
                    }
                }

                response = self.scheduler_client.update_schedule(
                    Name=task_schedule.unique_alias,
                    ScheduleExpression=schedule_expression,
                    FlexibleTimeWindow={'Mode': 'OFF'},
                    Target={
                        'Arn': task.identification,  
                        'Input': str(payload),
                        'RoleArn': task.target_role_arn  
                    }
                )

                self.logger.debug(f"[{self.__class__.__name__}] Event updated for task [{task_schedule.task_id}] with EventBridge [{task_schedule.unique_alias}]: [{response}]")
                return task_schedule.unique_alias

            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}] Failed to update event [{task_schedule.unique_alias}]: {e}")
                raise

        self.logger.warning(f"[{self.__class__.__name__}] No existing event to update. Creating a new one.")
        return self.register_event(task_schedule.task_table, task_schedule.execution_id)

    def delete_event(self, task_schedule: TaskSchedule):
        self.logger.debug(f"[{self.__class__.__name__}] Deleting event for task [{task_schedule.task_id}]: [{task_schedule.task_table.alias}]")

        if task_schedule.unique_alias:
            try:
                response = self.scheduler_client.delete_schedule(
                    Name=task_schedule.unique_alias
                )

                self.logger.debug(f"[{self.__class__.__name__}] Event deleted for task [{task_schedule.task_id}] with EventBridge [{task_schedule.unique_alias}]: [{response}]")
                return task_schedule.unique_alias

            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}] Failed to delete event [{task_schedule.unique_alias}]: {e}")
                raise

        self.logger.warning(f"[{self.__class__.__name__}] No existing event to delete.")