from datetime import datetime
from injector import inject
from jinja2 import Template
from config.constants import STATIC_SCHEDULE_IN_PROGRESS, STATIC_SCHEDULE_PENDENT
from models.task_schedule import TaskSchedule
from service.task_schedule_service import TaskScheduleService
from service.boto_service import BotoService
from models.table_execution import TableExecution
from service.task_table_service import TaskTableService
from service.cloud_watch_service import CloudWatchService
from service.table_service import TableService
from service.table_execution_service import TableExecutionService
from service.table_partition_exec_service import TablePartitionExecService
from models.tables import Tables
from models.dto.table_exec_dto import transform_to_table_exec_dto
from models.task_executor import TaskExecutor
from models.task_table import TaskTable
from logging import Logger
from typing import Any, Dict, Optional
import json

class TaskService:
    """
    Serviço responsável por gerenciar e acionar tabelas com base em suas dependências e configurações de execução.
    """
    @inject
    def __init__(
            self, 
            logger: Logger, 
            table_execution_service: TableExecutionService,
            table_service: TableService, 
            table_partition_exec_service: TablePartitionExecService,
            cloudwatch_service: CloudWatchService, 
            task_table_service: TaskTableService, 
            boto_service: BotoService,
            task_schedule_service: TaskScheduleService
        ):
        self.logger = logger
        self.table_execution_service = table_execution_service
        self.table_service = table_service
        self.table_partition_exec_service = table_partition_exec_service
        self.cloudwatch_service = cloudwatch_service
        self.task_table_service = task_table_service
        self.boto_service = boto_service
        self.task_schedule_service = task_schedule_service

    def trigger_tables(self, task_schedule_id: int, task_table_id: int, dependency_execution_id: int):
        """
        Aciona a execução de tabelas com base nas dependências e nas partições fornecidas.

        :param task_table_id: ID da tabela da tarefa a ser executada.
        :param dependency_execution_id: ID da execução da dependência.
        :param current_partitions: Dicionário contendo partições atuais (opcional).
        """
        start_time = datetime.utcnow()
        self.logger.info(f"[{self.__class__.__name__}] Trigger iniciado para task_table_id: {task_table_id}, dependency_execution_id: {dependency_execution_id}")
        try:
            task_schedule_list = self.task_schedule_service.query(
                id=task_schedule_id,
                status=STATIC_SCHEDULE_PENDENT
            )
            
            task_schedule = task_schedule_list[0] if task_schedule_list else None
            
            if not task_schedule:
                self.logger.warning(f"[{self.__class__.__name__}] Task Schedule não encontrado ou já processado.")
                return
            
            self.logger.debug(f"[{self.__class__.__name__}] Task Schedule encontrado: {task_schedule}")
            
            task_table = self.task_table_service.find(task_id=task_table_id)
            table = task_table.table
            dependency_execution = self.table_execution_service.find(id=dependency_execution_id)
            
            current_partitions = {
                p.partition.name: p.value
                for p in self.table_partition_exec_service.get_by_execution(dependency_execution.id)
            }
            
            self.logger.debug(f"[{self.__class__.__name__}][{table.name}] Partições atuais: {current_partitions}")
            dependencies_partitions = self._resolve_dependencies(table, current_partitions)
            
            if dependencies_partitions is None:
                self.logger.warning(f"[{self.__class__.__name__}][{table.name}] Dependências não resolvidas. Execução cancelada.")
                return

            self.process(task_schedule, task_table, dependency_execution, dependencies_partitions)

        except Exception as e:
            self.logger.exception(f"[{self.__class__.__name__}] Erro ao acionar tabelas: {str(e)}")
            self.cloudwatch_service.add_metric("TriggerTablesErrorCount", 1, "Count")

        finally:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self.cloudwatch_service.add_metric("TriggerTablesExecutionTime", execution_time, "Milliseconds")
            self.logger.info(f"[{self.__class__.__name__}] Trigger concluído em {execution_time:.2f} ms")

    def _resolve_dependencies(self, table: Tables, current_partitions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Resolve as dependências da tabela fornecida.

        :param table: Instância da tabela.
        :param current_partitions: Partições atuais fornecidas.
        :return: Dicionário com partições resolvidas ou None caso alguma dependência obrigatória falhe.
        """
        dependencies_partitions = {}
        for dependency in table.dependencies:
            execution = self.table_execution_service.get_latest_execution_with_restrictions(dependency.id, current_partitions)
            if not execution and dependency.is_required:
                self.logger.warning(f"[{self.__class__.__name__}][{table.name}] Dependência obrigatória não resolvida: {dependency.dependency_table.name}")
                return None

            dependencies_partitions[dependency.dependency_table.name] = {
                p.partition.name: p.value for p in self.table_partition_exec_service.get_by_execution(execution.id)
            } if execution else {}

        return dependencies_partitions

    def process(self, task_schedule: TaskSchedule, task_table: TaskTable, execution: TableExecution, dependencies_partitions: Dict[str, Any]):
        """
        Processa a execução da tarefa da tabela.

        :param task_schedule: Instância de TaskSchedule.
        :param task_table: Instância de TaskTable.
        :param execution: Instância de TableExecution associada.
        :param dependencies_partitions: Dicionário com as partições resolvidas das dependências.
        """
        self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Iniciando processamento.")
        try:
            task: TaskExecutor = task_table.task_executor
            table_info = transform_to_table_exec_dto(execution, dependencies_partitions)
                
            self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Parâmetros da tarefa: {task_table.params}({type (task_table.params)})")

            payload = self._interpolate_payload(
                task_table.params,
                table=task_table.table,
                partitions=dependencies_partitions,
                execution=execution,
                task=task,
                task_table=task_table,
                table_info=table_info,
            )

            payload["unique_alias"] = task_schedule.unique_alias

            method_map = {
                "stepfunction_process": self.stepfunction_process,
                "sqs_process": self.sqs_process,
                "glue_process": self.glue_process,
                "lambda_process": self.lambda_process,
                "eventbridge_process": self.eventbridge_process,
                "api_process": self.api_process
            }

            if task.method in method_map:
                response = method_map[task.method](execution, payload, task)
                self.cloudwatch_service.add_metric(f"{task.method.capitalize()}Count", 1, "Count")

                if response.get("ExecutionArn"):
                    self.logger.info(f"[{self.__class__.__name__}][{task_table.table.name}] Processamento iniciado: {response['ExecutionArn']}")

                    self.task_schedule_service.save({
                        "id": task_schedule.id,
                        "unique_alias": task_schedule.unique_alias,
                        "status": STATIC_SCHEDULE_IN_PROGRESS,
                        "execution_arn": response["ExecutionArn"]
                    })
            else:
                self.logger.error(f"[{self.__class__.__name__}][{task_table.table.name}] Método de processamento desconhecido: {task.method}")
                raise ValueError(f"Método de processamento não suportado: {task.method}")

        except Exception as e:
            self.logger.exception(f"[{self.__class__.__name__}][{task_table.table.name}] Erro no processamento: {str(e)}")
            self.cloudwatch_service.add_metric("ProcessingErrors", 1, "Count")


    def _interpolate_payload(self, payload: Optional[dict], **kwargs) -> dict:
        """
        Interpola o payload JSON fornecido usando o Jinja2.

        :param payload: Payload JSON base.
        :param kwargs: Contexto adicional para interpolação, com aliases explícitos.
        :return: Payload interpolado como um dicionário.
        """
        try:
            template = Template(json.dumps(payload))
            context = {
                alias: obj.dict() if hasattr(obj, 'dict') else obj
                for alias, obj in kwargs.items()
            }
            interpolated = template.render(**context)            
            return json.loads(interpolated)
        except Exception as e:
            self.logger.exception(f"[{self.__class__.__name__}] Erro ao interpolar payload: {e}")
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
            
            client = self.boto_service.get_client('stepfunctions')

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
            sqs_client = self.boto_service.get_client('sqs')
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
            glue_client = self.boto_service.get_client('glue')
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
            lambda_client = self.boto_service.get_client('lambda')
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
            eventbridge_client = self.boto_service.get_client('events')
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