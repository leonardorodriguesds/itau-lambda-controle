from jinja2 import Template
from models.tables import Tables
from models.dto.table_exec_dto import TableExecDTO, transform_to_table_exec_dto
from models.task_executor import TaskExecutor
from models.table_partition_exec import TablePartitionExec
from models.task_table import TaskTable
import boto3
import json
from logging import Logger
from typing import List, Optional
from models.table_execution import TableExecution

class TaskService:
    def __init__(self, session, logger: Logger):
        self.session = session
        self.logger = logger

    def process(self, task_table: TaskTable, execution: TableExecution, partition_execs: List[TablePartitionExec]):
        self.logger.debug(f"[TaskService] Processing task: [{task_table.alias}] for execution: [{execution.id}]")
        task: TaskExecutor = task_table.task_executor
        table_info: TableExecDTO = transform_to_table_exec_dto(execution, partition_execs)
        
        if task.method == "stepfunction_process":
            self.logger.debug(f"[TaskService] Processing task: [{task_table.alias}] for execution: [{execution.id}]")
            self.stepfunction_process(execution, self.interpolate_payload(task_table.params, table_info, execution, task, task_table, task_table.table), task_table.task_executor.stepfunction_arn)
            
    def interpolate_payload(
        self,
        payload: Optional[dict],
        table_exec: TableExecDTO,
        table_execution: TableExecution,
        task_executor: TaskExecutor,
        task_table: TaskTable,
        table: Tables
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
            self.logger.debug("[TaskService] Payload vazio. Retornando TableExecDTO como resposta.")
            return table_exec.dict()

        self.logger.debug(f"[TaskService] Interpolando payload com Jinja: {payload}")

        try:
            payload_str = json.dumps(payload)
            template = Template(payload_str)

            context = {
                "table_exec": table_exec.dict(),
                "table_execution": table_execution.dict(),
                "task_executor": task_executor.dict(),
                "task_table": task_table.dict(),
                "table": table.dict(),
            }

            interpolated_payload = template.render(**context)

            return json.loads(interpolated_payload)

        except Exception as e:
            self.logger.error(f"[TaskService] Erro ao interpolar payload: {e}")
            raise

    def stepfunction_process(self, execution: TableExecution, payload: dict, stepfunction_arn: str):
        """
        Chama uma Step Function enviando um payload JSON.

        :param execution: Instância da execução associada.
        :param payload: O payload JSON a ser enviado para a Step Function.
        :param stepfunction_arn: O ARN da Step Function que será invocada.
        """
        try:
            self.logger.debug(f"[TaskService] Invoking Step Function [{stepfunction_arn}] for execution: [{execution.id}]")
            
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
                stateMachineArn=stepfunction_arn,
                name=f"execution-{execution.id}",
                input=payload_json
            )

            self.logger.info(f"Step Function invoked successfully: {response['executionArn']}")
            return response

        except Exception as e:
            self.logger.error(f"Error invoking Step Function: {e}")
            raise
