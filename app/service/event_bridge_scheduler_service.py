from datetime import datetime, timedelta
import json
import re
from logging import Logger
from typing import Any, Dict
from injector import inject

from service.task_schedule_service import TaskScheduleService
from service.boto_service import BotoService
from models.table_execution import TableExecution
from models.task_table import TaskTable
from models.task_schedule import TaskSchedule

class EventBridgeSchedulerService:
    @inject
    def __init__(self, logger: Logger, boto_service: BotoService, task_schedule_service: TaskScheduleService):
        self.logger = logger
        self.scheduler_client = boto_service.get_client('scheduler')
        self.task_schedule_service: TaskScheduleService = task_schedule_service

    @staticmethod
    def dict_to_clean_string(input_dict: Dict[str, Any]) -> str:
        """
        Transforma um dicionário em uma string limpa no formato 'key=value-key=value'.
        """
        clean_items = []
        for key, value in input_dict.items():
            clean_key = re.sub(r'[^a-zA-Z0-9]', '', str(key))
            clean_value = re.sub(r'[^a-zA-Z0-9]', '', str(value))
            clean_items.append(f"{clean_key}={clean_value}")
        return '-'.join(clean_items)

    def generate_unique_alias(self, task_table: TaskTable, last_execution: TableExecution, partitions: Dict[str, Any]) -> str:
        """
        Gera um alias único para a execução da tarefa.
        """
        last_execution_id = last_execution.id if last_execution else "None"
        partitions_string = self.dict_to_clean_string(partitions) if partitions else "NoPartitions"
        return f"{task_table.table.name}-{task_table.alias}-{last_execution_id}-{partitions_string}"

    def build_event_payload(self, task_table: TaskTable, trigger_execution: TableExecution, partitions: Dict[str, Any], unique_alias: str) -> Dict[str, Any]:
        """
        Constrói o payload detalhado para o evento do scheduler.
        """
        return {
            "httpMethod": "POST",
            "path": "/trigger",
            "body": {
                "execution": {
                    "id": trigger_execution.id,
                    "source": trigger_execution.source,
                    "timestamp": trigger_execution.date_time.isoformat(),
                },
                "task_table": {
                    "id": task_table.id,
                    "alias": task_table.alias,
                },
                "partitions": partitions,
            },
            "metadata": {
                "unique_alias": unique_alias,
                "debounce_seconds": task_table.debounce_seconds,
            }
        }
        
    def check_event_exists(self, schedule_alias: str) -> bool:
        """
        Verifica se um evento existe no EventBridge Scheduler.
        
        :param schedule_alias: O alias do agendamento a ser verificado.
        :return: True se o evento existir, False caso contrário.
        """
        try:
            response = self.scheduler_client.list_schedules(NamePrefix=schedule_alias)
            schedules = response.get("Schedules", [])
            for schedule in schedules:
                if schedule.get("Name") == schedule_alias:
                    self.logger.info(f"[{self.__class__.__name__}] Event with alias '{schedule_alias}' exists.")
                    return True
        except self.scheduler_client.exceptions.ResourceNotFoundException:
            self.logger.info(f"[{self.__class__.__name__}] Event with alias '{schedule_alias}' does not exist.")
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error checking event existence: {e}")
            raise
        return False

    def register_or_postergate_event(self, task_table: TaskTable, trigger_execution: TableExecution, last_execution: TableExecution, table_last_execution: Dict[str, Any]):
        """
        Registra ou atualiza um evento no EventBridge para a execução da tarefa.
        """
        try:
            unique_alias = self.generate_unique_alias(task_table, last_execution, table_last_execution)
            self.logger.info(f"[{self.__class__.__name__}] Generated unique alias: {unique_alias}")
            possible_schedule = self.task_schedule_service.get_by_unique_alias_and_pendent(unique_alias)

            if possible_schedule and self.check_event_exists(possible_schedule.schedule_alias):
                self.logger.info(f"[{self.__class__.__name__}] Found existing schedule for alias: {unique_alias}. Updating event.")
                self.postergate_event(possible_schedule.schedule_alias, possible_schedule, trigger_execution, table_last_execution)
            else:
                self.logger.info(f"[{self.__class__.__name__}] No schedule found for alias: {unique_alias}. Registering new event.")
                self.register_event(unique_alias, task_table, trigger_execution, table_last_execution)

        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error during register_or_postergate_event: {e}")
            raise

    def register_event(self, unique_alias: str, task_table: TaskTable, trigger_execution: TableExecution, partitions: Dict[str, Any]):
        """
        Registra um novo evento no EventBridge.
        """
        try:
            self.logger.info(f"[{self.__class__.__name__}] Registering new event for task_table ID: {task_table.id}")
            schedule_execution_time = datetime.now() + timedelta(seconds=task_table.debounce_seconds)
            schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")

            payload = self.build_event_payload(task_table, trigger_execution, partitions, unique_alias)
            schedule_alias = f"{schedule_execution_time.strftime('%Y%m%d%H%M%S')}-{task_table.id}"[:64]

            response = self.scheduler_client.create_schedule(
                Name=schedule_alias,
                ScheduleExpression=schedule_expression,
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': task_table.task_executor.identification,
                    'Input': json.dumps(payload),
                    'RoleArn': task_table.task_executor.target_role_arn
                }
            )

            self.logger.info(f"[{self.__class__.__name__}] Event registered successfully: {response.get('ScheduleArn', 'unknown')}")

            self.task_schedule_service.save({
                "task_id": task_table.id,
                "unique_alias": unique_alias,
                "schedule_alias": schedule_alias,
                "table_execution_id": trigger_execution.id,
                "scheduled_execution_time": schedule_execution_time
            })
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Failed to register event: {e}")
            raise

    def postergate_event(self, schedule_alias: str, task_schedule: TaskSchedule, trigger_execution: TableExecution, partitions: Dict[str, Any]):
        """
        Atualiza um evento existente no EventBridge.
        """
        try:
            self.logger.info(f"[{self.__class__.__name__}] Postergating event for schedule ID: [{task_schedule.id}]: {schedule_alias} ({task_schedule.unique_alias})")	
            schedule_execution_time = datetime.now() + timedelta(seconds=task_schedule.task_table.debounce_seconds)
            schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")

            payload = self.build_event_payload(task_schedule.task_table, trigger_execution, partitions, task_schedule.unique_alias)

            response = self.scheduler_client.update_schedule(
                Name=schedule_alias,
                ScheduleExpression=schedule_expression,
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': task_schedule.task_table.task_executor.identification,
                    'Input': json.dumps(payload),
                    'RoleArn': task_schedule.task_table.task_executor.target_role_arn
                }
            )

            self.logger.info(f"[{self.__class__.__name__}] Event updated successfully: {response}")

            self.task_schedule_service.save({
                "id": task_schedule.id,
                "task_id": task_schedule.task_table.id,
                "unique_alias": task_schedule.unique_alias,
                "schedule_alias": schedule_alias,
                "table_execution_id": trigger_execution.id,
                "scheduled_execution_time": schedule_execution_time
            })
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Failed to update event: {e}")
            raise

    def delete_event(self, task_schedule: TaskSchedule):
        """
        Deleta um evento no EventBridge.
        """
        try:
            self.logger.info(f"[{self.__class__.__name__}] Deleting event for schedule ID: {task_schedule.id}")
            response = self.scheduler_client.delete_schedule(Name=task_schedule.schedule_alias)
            self.logger.info(f"[{self.__class__.__name__}] Event deleted successfully: {response}")
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Failed to delete event: {e}")
            raise
