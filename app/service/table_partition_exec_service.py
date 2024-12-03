import json
from logging import Logger
from typing import Any, Dict, List
from injector import inject
from datetime import datetime
from service.event_bridge_scheduler_service import EventBridgeSchedulerService
from service.partition_service import PartitionService
from service.table_service import TableService
from service.table_execution_service import TableExecutionService
from models.table_execution import TableExecution
from models.tables import Tables
from models.table_partition_exec import TablePartitionExec
from models.dto.table_partition_exec_dto import PartitionDTO, TablePartitionExecDTO
from exceptions.table_insert_error import TableInsertError
from repositories.table_partition_exec_repository import TablePartitionExecRepository

class TablePartitionExecService:
    @inject
    def __init__(self, logger: Logger, repository: TablePartitionExecRepository, table_service: TableService, table_execution_service: TableExecutionService, partition_service: PartitionService, event_bridge_scheduler_service: EventBridgeSchedulerService):
        self.logger = logger
        self.repository = repository
        self.table_service = table_service
        self.table_execution_service = table_execution_service
        self.partition_service = partition_service
        self.event_bridge_scheduler_service = event_bridge_scheduler_service
        
    def get_by_execution(self, execution_id: int) -> List[TablePartitionExec]:
        return self.repository.get_by_execution(execution_id)
                
    def trigger_tables(self, table_id: int):
        table = self.table_service.find(table_id=table_id)
        self.logger.debug(f"[{self.__class__.__name__}] Registring events for tables for: [{table.name}]")
        tables: List[Tables] = self.table_service.find_by_dependency(table_id)
        
        last_execution: TableExecution = self.table_execution_service.get_latest_execution(table_id)
        self.logger.debug(f"[{self.__class__.__name__}] Last execution for table [{table.name}]: [{last_execution.id}]")
        current_partitions = {
            p.partition.name: p.value
            for p in self.repository.get_by_execution(last_execution.id)
        }
            
        for table in tables:
            execution: TableExecution = self.table_execution_service.get_latest_execution_with_restrictions(table.id, current_partitions)
            
            table_target_partitions = {}
            
            if execution:
                table_target_partitions = {
                    p.partition.name: p.value
                    for p in self.repository.get_by_execution(execution.id)
                }
            
            for task in table.task_table:
                self.event_bridge_scheduler_service.register_or_postergate_event(task, last_execution, execution, table_target_partitions)
            
        
    def register_multiple_events(self, dtos: list[TablePartitionExecDTO]):
        """
        Registra múltiplos eventos de execução de partições para tabelas.
        """
        try:
            for dto in dtos:
                self.register_partitions_exec(dto)
            return {"message": f"{len(dtos)} eventos registrados com sucesso."}

        except Exception as e:
            raise TableInsertError(f"Erro ao registrar múltiplos eventos: {str(e)}")

    def register_partitions_exec(self, dto: TablePartitionExecDTO):
        """
        Registra execuções de partições para uma tabela, usando ID ou nome.
        Valida:
        1. Todas as partições obrigatórias devem estar presentes.
        2. Pode conter qualquer conjunto de partições opcionais.
        3. Todas as partições fornecidas devem estar associadas à tabela.
        4. Atualiza automaticamente a versão mais recente para cada conjunto `table x partition`.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Registering partitions exec for: {dto}")
        try:
            table = None
            if dto.table_id:
                table = self.table_service.find(table_id=dto.table_id)
            elif dto.table_name:
                table = self.table_service.find(table_name=dto.table_name)

            if not table:
                raise TableInsertError(
                    f"Tabela com ID '{dto.table_id}' ou nome '{dto.table_name}' não encontrada."
                )

            partitions = {p.id: p for p in table.partitions}
            required_partitions = {p.id for p in table.partitions if p.is_required}

            resolved_partitions = []
            for partition in dto.partitions:
                if partition.partition_id:
                    if partition.partition_id not in partitions:
                        raise TableInsertError(
                            f"Partição com ID '{partition.partition_id}' não está associada à tabela '{table.name}'."
                        )
                    resolved_partitions.append(partition)
                elif partition.partition_name:
                    matched_partition = next(
                        (p for p in partitions.values() if p.name == partition.partition_name), None
                    )
                    if not matched_partition:
                        raise TableInsertError(
                            f"Partição com nome '{partition.partition_name}' não está associada à tabela '{table.name}'."
                        )
                    resolved_partitions.append(
                        PartitionDTO(
                            partition_id=matched_partition.id,
                            partition_name=matched_partition.name,
                            value=partition.value,
                        )
                    )
                else:
                    raise TableInsertError("Cada partição deve ter um ID ou um nome.")

            provided_partitions = {p.partition_id for p in resolved_partitions}

            missing_required_partitions = required_partitions - provided_partitions
            if missing_required_partitions:
                missing_names = [
                    partitions[pid].name for pid in missing_required_partitions
                ]
                raise TableInsertError(
                    f"As seguintes partições obrigatórias estão faltando: {', '.join(missing_names)}"
                )

            new_execution = self.table_execution_service.create_execution(table.id, dto.source)

            for partition in resolved_partitions:
                new_entry = TablePartitionExec(
                    table_id=table.id,
                    partition_id=partition.partition_id,
                    value=partition.value,
                    execution_date=datetime.utcnow(),
                    execution_id=new_execution.id  
                )
                self.repository.save(new_entry)

            self.trigger_tables(new_execution.table_id)
            return {"message": "Table partition execution entries registered successfully."}

        except Exception as e:
            raise TableInsertError(f"Erro ao registrar execuções de partições: {str(e)}")

