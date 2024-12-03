from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import get_native_id
from injector import inject
from jinja2 import Template
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
    def __init__(self, logger: Logger, table_execution_service: TableExecutionService, table_service: TableService, table_partition_exec_service: TablePartitionExecService):
        self.logger = logger
        self.table_execution_service = table_execution_service
        self.table_service = table_service
        self.table_partition_exec_service = table_partition_exec_service
        
    def trigger_tables(self, table_id: int, current_partitions: Dict[str, Any] = None):
        table = self.table_service.find(table_id=table_id)
        self.logger.debug(f"[{self.__class__.__name__}] Triggering tables for: [{table.name}]")
        tables: List[Tables] = self.table_service.find_by_dependency(table_id)
        
        last_execution: TableExecution = self.table_execution_service.get_latest_execution(table_id)
        if not current_partitions:
            current_partitions = {
                p.partition.name: p.value
                for p in self.table_partition_exec_service.get_by_execution(last_execution.id)
            }
        
        def process_table(table: Tables):
            """
            Processa cada tabela de forma independente dentro de uma thread.
            """
            thread_id = get_native_id() 
            start_time = datetime.now() 
            self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] Processing table [{table.name}] in thread")
            try:
                def resolve_dependency(dependency: Dependencies) -> Dict[str, Any]:
                    """
                    Resolve as dependências recursivamente.
                    """
                    execution: TableExecution = self.table_execution_service.get_latest_execution_with_restrictions(dependency.id, current_partitions)
                    if not execution:
                        self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] No execution found for dependency [{dependency.dependency_table.name}]")
                        return None
                    self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] Execution found for dependency [{dependency.dependency_table.name}]: [{execution.id}]")
                    return {
                        p.partition.name: p.value for p in self.table_partition_exec_service.get_by_execution(execution.id)
                    }
                    
                dependencies_partitions = {}
                for dependency in table.dependencies:
                    resolved = resolve_dependency(dependency)
                    if resolved is None:
                        self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] Failed to resolve dependency [{dependency.dependency_table.name}]")
                        return
                    
                    dependencies_partitions[dependency.dependency_table.name] = resolved


                self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] Dependencies partitions for table [{table.name}]: {dependencies_partitions}")
                
                for dep in table.dependencies:
                    if dep.is_required and not dependencies_partitions[dep.dependency_table.name]:
                        self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] No execution found for dependency [{dep}]")
                        return
                    
                for task in table.task_table:
                    self.process(task, last_execution, dependencies_partitions)
            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}][{thread_id}] Error processing table [{table.name}]: {e}")
            finally:
                end_time = datetime.now()
                self.logger.debug(f"[{self.__class__.__name__}][{thread_id}] Table [{table.name}] processed in {end_time - start_time}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(process_table, tables)      

    def process(self, task_table: TaskTable, execution: TableExecution, dependencies_executions: Dict[str, Dict[str, Any]]):
        self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Processing task: [{task_table.alias}] for execution: [{execution.id}]")
        task: TaskExecutor = task_table.task_executor
        
        table_info: TableExecDTO = transform_to_table_exec_dto(execution, dependencies_executions)
        
        if task.method == "stepfunction_process":
            self.logger.debug(f"[{self.__class__.__name__}][{task_table.table.name}] Processing stepfunction: [{task_table.alias}] for execution: [{execution.id}]")
            self.stepfunction_process(
                execution, 
                self.interpolate_payload(
                    task_table.params,
                    table_info, 
                    execution, 
                    task, 
                    task_table, 
                    task_table.table,
                    dependencies_executions
                ), 
                task
            )
            
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
