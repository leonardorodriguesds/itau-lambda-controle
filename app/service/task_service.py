from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import get_native_id
from injector import inject
from jinja2 import Template
from service.task_table_service import TaskTableService
from service.cloud_watch_service import CloudWatchService
from service.table_service import TableService
from models.dependencies import Dependencies
from service.table_execution_service import TableExecutionService
from service.table_partition_exec_service import TablePartitionExecService
from models.tables import Tables
from models.dto.table_exec_dto import TableExecDTO, transform_to_table_exec_dto
from models.task_executor import TaskExecutor
from models.task_table import TaskTable
import boto3
import json
from logging import Logger
from typing import Any, Dict, List, Optional
from models.table_execution import TableExecution

class TaskService:
    @inject
    def __init__(self, logger: Logger, table_execution_service: TableExecutionService, table_service: TableService, table_partition_exec_service: TablePartitionExecService, cloudwatch_service: CloudWatchService, task_table_service: TaskTableService):
        self.logger = logger
        self.table_execution_service = table_execution_service
        self.table_service = table_service
        self.table_partition_exec_service = table_partition_exec_service
        self.cloudwatch_service = cloudwatch_service
        self.task_table_service = task_table_service

    def trigger_tables(self, task_table_id: int, dependency_execution_id: int, current_partitions: Dict[str, Any] = None):
        start_time = datetime.utcnow()
        error_count = 0
        self.logger.info(f"[{self.__class__.__name__}] Triggering task table [{task_table_id}] with partitions: {current_partitions}")
        
        task_table: TaskTable = self.task_table_service.find(task_id=task_table_id)
        table: Tables = task_table.table
        dependency_execution: TableExecution = self.table_execution_service.find(id=dependency_execution_id)
        
        def resolve_dependency(dependency: Dependencies) -> Dict[str, Any]:
            execution: TableExecution = self.table_execution_service.get_latest_execution_with_restrictions(dependency.id, current_partitions)
            if not execution:
                self.logger.debug(f"[{self.__class__.__name__}][{table.name}] No execution found for dependency [{dependency.dependency_table.name}]")
                return None
            self.logger.debug(f"[{self.__class__.__name__}][{table.name}] Execution found for dependency [{dependency.dependency_table.name}]: [{execution.id}]")
            return {
                p.partition.name: p.value for p in self.table_partition_exec_service.get_by_execution(execution.id)
            }

        dependencies_partitions = {}
        for dependency in table.dependencies:
            resolved = resolve_dependency(dependency)
            if resolved is None:
                self.logger.debug(f"[{self.__class__.__name__}][{table.name}] Failed to resolve dependency [{dependency.dependency_table.name}]")
                return

            dependencies_partitions[dependency.dependency_table.name] = resolved

        self.logger.debug(f"[{self.__class__.__name__}][{table.name}] Dependencies partitions for table [{table.name}]: {dependencies_partitions}")

        for dep in table.dependencies:
            if dep.is_required and not dependencies_partitions[dep.dependency_table.name]:
                self.logger.debug(f"[{self.__class__.__name__}][{table.name}] No execution found for dependency [{dep}]")
                return

        self.process(task_table, dependency_execution, dependencies_partitions)
        
        total_execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        self.cloudwatch_service.add_metric(name="TriggerTablesExecutionTime", value=total_execution_time, unit="Milliseconds")
        self.cloudwatch_service.add_metric(name="TriggerTablesErrorCount", value=error_count, unit="Count")  

    def process(self, task_table: TaskTable, execution: TableExecution, dependencies_executions: Dict[str, Dict[str, Any]]):
        self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Processing task: [{task_table.alias}] for execution: [{execution.id}]")
        task: TaskExecutor = task_table.task_executor

        table_info: TableExecDTO = transform_to_table_exec_dto(execution, dependencies_executions)

        payload = self.interpolate_payload(
            task_table.params,
            table_info, 
            execution, 
            task, 
            task_table, 
            task_table.table,
            dependencies_executions
        )

        try:
            if task.method == "stepfunction_process":
                self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Processing stepfunction: [{task_table.alias}]")
                self.stepfunction_process(execution, payload, task)
                self.cloudwatch_service.add_metric(name="StepFunctionProcesses", value=1, unit="Count")
            elif task.method == "sqs_process":
                self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Processing SQS: [{task_table.alias}]")
                self.sqs_process(payload, task)
                self.cloudwatch_service.add_metric(name="SQSProcesses", value=1, unit="Count")
            elif task.method == "glue_process":
                self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Processing Glue Job: [{task_table.alias}]")
                self.glue_process(payload, task)
                self.cloudwatch_service.add_metric(name="GlueProcesses", value=1, unit="Count")
            elif task.method == "lambda_process":
                self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Invoking Lambda: [{task_table.alias}]")
                self.lambda_process(payload, task)
                self.cloudwatch_service.add_metric(name="LambdaProcesses", value=1, unit="Count")
            elif task.method == "eventbridge_process":
                self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Creating EventBridge Rule: [{task_table.alias}]")
                self.eventbridge_process(payload, task)
                self.cloudwatch_service.add_metric(name="EventBridgeProcesses", value=1, unit="Count")
            elif task.method == "api_process":
                self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Calling API: [{task_table.alias}]")
                self.api_process(payload, task)
                self.cloudwatch_service.add_metric(name="APIProcesses", value=1, unit="Count")
            else:
                self.logger.error(f"[{self.__class__.__name__}][{task_table.table.name}] Unknown processing method: [{task.method}]")
                raise ValueError(f"Unsupported task method: {task.method}")
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}][{task_table.table.name}] Error in process method: {e}")
            raise

            
    def interpolate_payload(
        self,
        payload: Optional[dict],
        table_exec: TableExecDTO,
        table_execution: TableExecution,
        task_executor: TaskExecutor,
        task_table: TaskTable,
        table: Tables,
        dependencies_executions: Dict[str, Dict[str, Any]]
    ) -> dict:
        """
        Interpola variáveis em um payload JSON usando informações de TableExecDTO, TableExecution, TaskExecutor, TaskTable e Table.

        :param payload: O payload JSON a ser interpolado.
        :param table_exec: Instância de TableExecDTO contendo informações da execução da tabela.
        :param table_execution: Instância de TableExecution.
        :param task_executor: Instância de TaskExecutor.
        :param task_table: Instância de TaskTable.
        :param table: Instância de Table.
        :return: Payload interpolado com valores das variáveis, ou TableExecDTO se o payload for vazio.
        """
        if not payload:
            self.logger.debug(f"[{self.__class__.__name__}] Payload vazio. Retornando TableExecDTO como resposta.")
            return table_exec.dict()

        self.logger.debug(f"[{self.__class__.__name__}] Interpolando payload com Jinja: {payload}")

        try:
            payload_str = json.dumps(payload)
            self.logger.debug(f"[{self.__class__.__name__}] Payload JSON: {payload_str}")
            template = Template(payload_str)

            context = {
                "table_exec": table_exec.dict(),
                "table_execution": table_execution.dict(),
                "task_executor": task_executor.dict(),
                "task_table": task_table.dict(),
                "table": table.dict(),
                "dependencies": dependencies_executions
            }
            
            self.logger.debug(f"[{self.__class__.__name__}] Contexto de interpolação: [{context}]")

            interpolated_payload = template.render(**context)
            
            self.logger.debug(f"[{self.__class__.__name__}] Payload: [{interpolated_payload}]")

            return json.loads(interpolated_payload)

        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Erro ao interpolar payload: {e}")
            raise

    def stepfunction_process(self, execution: TableExecution, payload: dict, task_executor: TaskExecutor):
        """
        Chama uma Step Function enviando um payload JSON.

        :param execution: Instância da execução associada.
        :param payload: O payload JSON a ser enviado para a Step Function.
        :param stepfunction_arn: O ARN da Step Function que será invocada.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Invoking Step Function [{task_executor.identification}] for execution: [{execution.id}]")
            
            client = boto3.client('stepfunctions')

            payload_with_metadata = {
                "execution_id": execution.id,
                "table_id": execution.table_id,
                "source": execution.source,
                "date_time": execution.date_time.isoformat(),
                "payload": payload 
            }

            payload_json = json.dumps(payload_with_metadata)
            
            self.logger.info(f"Payload JSON: {payload_json}")

            response = client.start_execution(
                stateMachineArn=task_executor.identification,
                name=f"execution-{execution.id}",
                input=payload_json
            )

            self.logger.info(f"Step Function invoked successfully: {response['executionArn']}")
            return response

        except Exception as e:
            self.logger.error(f"Error invoking Step Function: {e}")
            raise
        
    def sqs_process(self, payload: dict, task_executor: TaskExecutor):
        try:
            sqs_client = boto3.client('sqs')
            response = sqs_client.send_message(
                QueueUrl=task_executor.identification,
                MessageBody=json.dumps(payload)
            )
            self.logger.info(f"SQS message sent successfully: {response['MessageId']}")
        except Exception as e:
            self.logger.error(f"Error sending SQS message: {e}")
            raise
        
    def glue_process(self, payload: dict, task_executor: TaskExecutor):
        try:
            glue_client = boto3.client('glue')
            response = glue_client.start_job_run(
                JobName=task_executor.identification,
                Arguments=payload
            )
            self.logger.info(f"Glue job started successfully: {response['JobRunId']}")
        except Exception as e:
            self.logger.error(f"Error starting Glue job: {e}")
            raise
        
    def lambda_process(self, payload: dict, task_executor: TaskExecutor):
        try:
            lambda_client = boto3.client('lambda')
            response = lambda_client.invoke(
                FunctionName=task_executor.identification,
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            self.logger.info(f"Lambda function invoked successfully: {response['StatusCode']}")
        except Exception as e:
            self.logger.error(f"Error invoking Lambda function: {e}")
            raise

    def eventbridge_process(self, payload: dict, task_executor: TaskExecutor):
        try:
            eventbridge_client = boto3.client('events')
            response = eventbridge_client.put_events(
                Entries=[
                    {
                        'Source': task_executor.identification,
                        'DetailType': 'Table Process Event',
                        'Detail': json.dumps(payload)
                    }
                ]
            )
            self.logger.info(f"EventBridge event sent successfully: {response['Entries']}")
        except Exception as e:
            self.logger.error(f"Error sending EventBridge event: {e}")
            raise

    def api_process(self, payload: dict, task_executor: TaskExecutor):
        try:
            import requests
            response = requests.post(task_executor.identification, json=payload)
            response.raise_for_status()
            self.logger.info(f"API called successfully: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Error calling API: {e}")
            raise

