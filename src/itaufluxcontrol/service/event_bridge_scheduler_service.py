from datetime import datetime, timedelta
import json
import re
from logging import Logger
from typing import Any, Dict
from injector import inject

from src.itaufluxcontrol.config.constants import STATIC_APPROVE_STATUS_PENDING, STATIC_SCHEDULE_COMPLETED, STATIC_SCHEDULE_FAILED, STATIC_SCHEDULE_PENDENT, STATIC_SCHEDULE_WAITING_APPROVAL
from src.itaufluxcontrol.models.tables import Tables
from src.itaufluxcontrol.service.approval_status_service import ApprovalStatusService
from src.itaufluxcontrol.service.task_schedule_service import TaskScheduleService
from src.itaufluxcontrol.service.boto_service import BotoService
from src.itaufluxcontrol.models.table_execution import TableExecution
from src.itaufluxcontrol.models.task_table import TaskTable
from src.itaufluxcontrol.models.task_schedule import TaskSchedule

class EventBridgeSchedulerService:
    @inject
    def __init__(self, logger: Logger, boto_service: BotoService, task_schedule_service: TaskScheduleService, approval_status_service: ApprovalStatusService):
        self.logger = logger
        self.scheduler_client = boto_service.get_client('scheduler')
        self.task_schedule_service: TaskScheduleService = task_schedule_service
        self.approval_status_service: ApprovalStatusService = approval_status_service

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
    
    def finish_with_success(self, task_schedule_id: int, table_execution: TableExecution):
        """
        Finaliza um agendamento com sucesso.
        """
        task_schedule = self.task_schedule_service.find(task_schedule_id)
        if task_schedule:
            self.logger.info(f"[{self.__class__.__name__}] Finishing task schedule with success: {task_schedule_id}")
            task_schedule.status = STATIC_SCHEDULE_COMPLETED
            task_schedule.result_execution_id = table_execution.id
            self.task_schedule_service.save(task_schedule.dict())
        else:
            self.logger.error(f"[{self.__class__.__name__}] Task schedule not found: {task_schedule_id}")
            
    def finish_with_error(self, task_schedule_id: int, error_message: str):
        """
        Finaliza um agendamento com erro.
        """
        task_schedule = self.task_schedule_service.find(task_schedule_id)
        if task_schedule:
            self.logger.info(f"[{self.__class__.__name__}] Finishing task schedule with error: {task_schedule_id}")
            task_schedule.status = STATIC_SCHEDULE_FAILED
            task_schedule.error_message = error_message
            self.task_schedule_service.save(task_schedule)
        else:
            self.logger.error(f"[{self.__class__.__name__}] Task schedule not found: {task_schedule_id}")

    def generate_unique_alias(self, task_table: TaskTable, last_execution: TableExecution, partitions: Dict[str, Any]) -> str:
        """
        Gera um alias único para a execução da tarefa.
        """
        last_execution_id = last_execution.id if last_execution else "None"
        partitions_string = self.dict_to_clean_string(partitions) if partitions else "NoPartitions"
        return f"{task_table.table.name}-{task_table.alias}-{last_execution_id}-{partitions_string}"

    def build_event_payload(self, task_table: TaskTable, trigger_execution: TableExecution, task_schedule: TaskSchedule, partitions: Dict[str, Any]) -> Dict[str, Any]:
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
                "task_schedule": {
                    "id": task_schedule.id,
                    "schedule_alias": task_schedule.schedule_alias,
                    "unique_alias": task_schedule.unique_alias,
                },
                "partitions": partitions,
            },
            "metadata": {
                "unique_alias": task_schedule.unique_alias,
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
                self.logger.info(f"[{self.__class__.__name__}] Checking event existence: {schedule.get('Name')}={schedule_alias}")
                if schedule.get("Name").lower() == schedule_alias.lower():
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
                self.register_event(possible_schedule, unique_alias, task_table, trigger_execution, table_last_execution)

        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error during register_or_postergate_event: {e}")
            raise
        
    def schedule(self, task_schedule: TaskSchedule):
        """
        Agenda um evento no EventBridge.
        """
        try:
            self.logger.info(f"[{self.__class__.__name__}] Scheduling event for schedule ID: {task_schedule.id}")
            if self.check_event_exists(task_schedule.schedule_alias):
                self.logger.info(f"[{self.__class__.__name__}] Found existing schedule for alias: {task_schedule.schedule_alias}. Updating event.")
                self._update_event(task_schedule)
            else:
                self.logger.info(f"[{self.__class__.__name__}] No schedule found for alias: {task_schedule.schedule_alias}. Registering new event.")
                self._register_event(task_schedule)
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Failed to schedule event: {e}")
            raise

    def register_event(self, possible_schedule: TaskSchedule, unique_alias: str, task_table: TaskTable, trigger_execution: TableExecution, partitions: Dict[str, Any]):
        """
        Registra um novo evento no EventBridge.
        """
        try:
            table: Tables = task_table.table
            self.logger.info(f"[{self.__class__.__name__}] Registering new event for task_table ID: {task_table.id}")
            schedule_execution_time = datetime.now() + timedelta(seconds=task_table.debounce_seconds)
            
            schedule_alias = f"{schedule_execution_time.strftime('%Y%m%d%H%M%S')}-{task_table.id}"[:64]
            
            task_schedule_dict = {
                "id": possible_schedule.id if possible_schedule else None,
                "task_id": task_table.id,
                "unique_alias": unique_alias,
                "schedule_alias": schedule_alias,
                "table_execution_id": trigger_execution.id,
                "scheduled_execution_time": schedule_execution_time,
                "partitions": json.dumps(partitions) if partitions else None,
                "status": STATIC_SCHEDULE_PENDENT
            }
            
            if table.requires_approval:
                task_schedule_dict["status"] = STATIC_SCHEDULE_WAITING_APPROVAL
            
            task_schedule = self.task_schedule_service.save(task_schedule_dict)
            
            if table.requires_approval:
                possible_task_approval = self.approval_status_service.find_by_task_schedule_id(task_schedule.id)
                self.approval_status_service.save({
                    "id": possible_task_approval.id if possible_task_approval else None,
                    "task_schedule_id": task_schedule.id,
                    "status": STATIC_APPROVE_STATUS_PENDING
                })
            else:
                self._register_event(task_schedule)            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Failed to register event: {e}")
            raise
        
    def _register_event(self, task_schedule: TaskSchedule):
        trigger_execution = task_schedule.table_execution
        partitions = json.loads(task_schedule.partitions) if task_schedule.partitions else {}
        schedule_alias = task_schedule.schedule_alias
        task_table = task_schedule.task_table
        payload = self.build_event_payload(task_table, trigger_execution, task_schedule, partitions)
        
        schedule_execution_time = datetime.now() + timedelta(seconds=task_table.debounce_seconds)
        schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")
        
        task_schedule.scheduled_execution_time = schedule_execution_time
        task_schedule.status = STATIC_SCHEDULE_PENDENT
        self.task_schedule_service.save(task_schedule.dict())

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

    def postergate_event(self, schedule_alias: str, task_schedule: TaskSchedule, trigger_execution: TableExecution, partitions: Dict[str, Any]):
        """
        Atualiza um evento existente no EventBridge.
        """
        try:
            self.logger.info(f"[{self.__class__.__name__}] Postergating event for schedule ID: [{task_schedule.id}]: {schedule_alias} ({task_schedule.unique_alias})")	
            
            table: Tables = task_schedule.task_table.table
            schedule_execution_time = datetime.now() + timedelta(seconds=task_schedule.task_table.debounce_seconds)
            
            task_schedule_dict = {
                "id": task_schedule.id,
                "task_id": task_schedule.task_table.id,
                "unique_alias": task_schedule.unique_alias,
                "schedule_alias": schedule_alias,
                "table_execution_id": trigger_execution.id,
                "scheduled_execution_time": schedule_execution_time,
                "partitions": json.dumps(partitions) if partitions else None,
                "status": STATIC_SCHEDULE_PENDENT
            }        
            
            if table.requires_approval:
                task_schedule_dict["status"] = STATIC_SCHEDULE_WAITING_APPROVAL
            
            task_schedule = self.task_schedule_service.save(task_schedule_dict)   
            
            if table.requires_approval:
                possible_task_approval = self.approval_status_service.find_by_task_schedule_id(task_schedule.id)
                self.approval_status_service.save({
                    "id": possible_task_approval.id if possible_task_approval else None,
                    "task_schedule_id": task_schedule.id,
                    "status": STATIC_APPROVE_STATUS_PENDING
                })  
            else:
                self._update_event(task_schedule)       
        
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Failed to update event: {e}")
            raise
        
    def _update_event(self, task_schedule: TaskSchedule):
        trigger_execution = task_schedule.table_execution
        partitions = json.loads(task_schedule.partitions) if task_schedule.partitions else {}
        schedule_alias = task_schedule.schedule_alias
        
        schedule_execution_time = datetime.now() + timedelta(seconds=task_schedule.task_table.debounce_seconds)
        schedule_expression = schedule_execution_time.strftime("cron(%M %H %d %m ? *)")
        
        payload = self.build_event_payload(task_schedule.task_table, trigger_execution, task_schedule, partitions)
        
        task_schedule.scheduled_execution_time = schedule_execution_time
        task_schedule.status = STATIC_SCHEDULE_PENDENT
        self.task_schedule_service.save(task_schedule.dict())
        possible_task_approval = self.approval_status_service.find_by_task_schedule_id(task_schedule.id)
        
        if possible_task_approval:
            self.approval_status_service.approve(possible_task_approval.id, 'automatic')

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
