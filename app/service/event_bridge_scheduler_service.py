from datetime import datetime, timedelta
import json
from logging import Logger
import os
import re
from typing import Any, Dict
from botocore.session import Session
from injector import inject

from service.task_schedule_service import TaskScheduleService
from service.boto_service import BotoService
from models.table_execution import TableExecution
from models.task_executor import TaskExecutor
from models.task_table import TaskTable
from models.task_schedule import TaskSchedule

from datetime import datetime, timedelta
from logging import Logger

class EventBridgeSchedulerService:
    @inject
    def __init__(self, logger: Logger, boto_service: BotoService, task_schedule_service: TaskScheduleService):
        self.logger = logger
        self.scheduler_client = boto_service.get_client(
            'scheduler'
        )
        self.task_schedule_service = task_schedule_service
        
    import re

    @staticmethod
    def dict_to_clean_string(input_dict):
        """
        Transforma um dicionário em uma string no formato 'key=value-key=value',
        removendo caracteres especiais dos valores.
        """
        if not isinstance(input_dict, dict):
            raise ValueError("O input deve ser um dicionário.")
        
        clean_items = []
        for key, value in input_dict.items():
            clean_key = re.sub(r'[^a-zA-Z0-9]', '', str(key))
            clean_value = re.sub(r'[^a-zA-Z0-9]', '', str(value))
            clean_items.append(f"{clean_key}={clean_value}")
        
        clean_string = '-'.join(clean_items)
        return clean_string

    
    def generate_unique_alias(self, task_table: TaskTable, last_execution: TableExecution, partitions: Dict[str, Any]):
        """
        Gera um alias único para a execução da tarefa.
        """
        last_execution_id = last_execution.id if last_execution else "None"
        partitions_string = self.dict_to_clean_string(partitions) if partitions else "NoPartitions"
        
        return f"{task_table.table.name}-{task_table.alias}-{last_execution_id}-{partitions_string}"

    
    def register_or_postergate_event(self, task_table: TaskTable, trigger_execution: TableExecution, last_execution: TableExecution, partitions: Dict[str, Any]):
        """
        Registra ou atualiza um evento no EventBridge para a execução da tarefa.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Registering or postergating event for task [{task_table.id}]: [{task_table.alias}]")
        
        unique_alias = self.generate_unique_alias(task_table, last_execution, partitions)
        self.logger.debug(f"[{self.__class__.__name__}] Unique alias for task [{task_table.id}]: [{unique_alias}]")
        possible_schedule = self.task_schedule_service.get_by_unique_alias_and_pendent(unique_alias)
        self.logger.debug(f"[{self.__class__.__name__}] Possible schedule for task [{task_table.id}]: [{possible_schedule}]")
        if possible_schedule:
            self.postergate_event(possible_schedule, trigger_execution)
        else:
            self.register_event(task_table, trigger_execution, last_execution, partitions)

    def register_event(self, task_table: TaskTable, trigger_execution: TableExecution, last_execution: TableExecution, partitions: Dict[str, Any]):
        self.logger.debug(f"[{self.__class__.__name__}] Registering event for task [{task_table.id}]: [{task_table.alias}]")
        
        task: TaskExecutor = task_table.task_executor
        unique_alias = self.generate_unique_alias(task_table, last_execution, partitions)
        self.logger.debug(f"[{self.__class__.__name__}] Unique alias for task [{task_table.id}]: [{unique_alias}]")
        
        schedule_execution_time = datetime.now() + timedelta(hours=task_table.debounce_seconds)
        schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")

        payload = {
            "event": "trigger",
            "body": {
                "execution": trigger_execution.id,
                "task_table": task_table.id
            }
        }

        try:
            schedule_alias = f"{datetime.now().strftime('%Y%m%d%H%M%S')}{task_table.table.name}{task_table.id}"[:64]
            self.logger.debug(f"[{self.__class__.__name__}] Registering event for task [{task_table.id}] with alias [{schedule_alias}]")
            response = self.scheduler_client.create_schedule(
                Name=schedule_alias,
                ScheduleExpression=schedule_expression,
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': task.identification,  
                    'Input': json.dumps(payload),
                    'RoleArn': task.target_role_arn  
                }
            )

            event_id = response.get('ScheduleArn', 'unknown')
            self.logger.debug(f"[{self.__class__.__name__}] Event registered for task [{task_table.id}] with EventBridge ID [{event_id}]")
            
            self.task_schedule_service.save({
                "task_id": task_table.id,
                "unique_alias": unique_alias,
                "schedule_alias": schedule_alias,
                "table_execution_id": trigger_execution.id,
                "scheduled_execution_time": schedule_execution_time
            })
            return unique_alias
        except Exception as e:
            self.logger.error(f"Failed to register event: {e}")
            raise

    def postergate_event(self, task_schedule: TaskSchedule, trigger_execution: TableExecution):
        self.logger.debug(f"[{self.__class__.__name__}] Postergating event for task [{task_schedule.task_id}]: [{task_schedule.task_table.alias}]")
        
        task: TaskExecutor = task_schedule.task_table.task_executor
        if task_schedule.unique_alias:
            try:
                schedule_execution_time = datetime.now() + timedelta(seconds=task_schedule.task_table.debounce_seconds)
                schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")
                task_table: TaskTable = task_schedule.task_table
                
                payload = {
                    "event": "trigger",
                    "body": {
                        "execution": trigger_execution.id,
                        "task_table": task_table.id
                    }
                }
                
                schedule_alias = task_schedule.schedule_alias
                response = self.scheduler_client.update_schedule(
                    Name=schedule_alias,
                    ScheduleExpression=schedule_expression,
                    FlexibleTimeWindow={'Mode': 'OFF'},
                    Target={
                        'Arn': task.identification,  
                        'Input': json.dumps(payload),
                        'RoleArn': task.target_role_arn  
                    }
                )

                self.logger.debug(f"[{self.__class__.__name__}] Event updated for task [{task_schedule.task_id}] with EventBridge [{task_schedule.unique_alias}]: [{response}]")
                
                self.task_schedule_service.save({
                    "id": task_schedule.id,
                    "task_id": task_table.id,
                    "unique_alias": task_schedule.unique_alias,
                    "schedule_alias": schedule_alias,
                    "table_execution_id": trigger_execution.id,
                    "scheduled_execution_time": schedule_execution_time
                })
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