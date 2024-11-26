from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from threading import get_native_id
from typing import Any, Dict, List
from injector import inject
from datetime import datetime
from models.dependencies import Dependencies
from service.task_service import TaskService
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
    def __init__(self, logger: Logger, repository: TablePartitionExecRepository, table_service: TableService, table_execution_service: TableExecutionService, partition_service: PartitionService, task_service: TaskService):
        self.logger = logger
        self.repository = repository
        self.table_service = table_service
        self.table_execution_service = table_execution_service
        self.partition_service = partition_service
        self.task_service = task_service
                
    def trigger_tables(self, table_id: int, current_partitions: Dict[str, Any] = None):
        table = self.table_service.find(table_id=table_id)
        self.logger.debug(f"[{self.__class__.__name__}] Triggering tables for: [{table.name}]")
        tables: List[Tables] = self.table_service.find_by_dependency(table_id)
        
        last_execution: TableExecution = self.table_execution_service.get_latest_execution(table_id)
        if not current_partitions:
            current_partitions = {
                p.partition.name: p.value
                for p in self.repository.get_by_execution(last_execution.id)
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
                        p.partition.name: p.value for p in self.repository.get_by_execution(execution.id)
                    }
                    
                dependencies_partitions = {
                    d.dependency_table.name: resolve_dependency(d) for d in table.dependencies
                }

                self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] Dependencies partitions for table [{table.name}]: {dependencies_partitions}")
                
                for dep in table.dependencies:
                    if dep.is_required and not dependencies_partitions[dep.dependency_table.name]:
                        self.logger.debug(f"[{self.__class__.__name__}][{thread_id}][{table.name}] No execution found for dependency [{dep}]")
                        return
                    
                for task in table.task_table:
                    self.task_service.process(task, table, last_execution, dependencies_partitions)
            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}][{thread_id}] Error processing table [{table.name}]: {e}")
            finally:
                end_time = datetime.now()
                self.logger.debug(f"[{self.__class__.__name__}][{thread_id}] Table [{table.name}] processed in {end_time - start_time}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(process_table, tables)      
            
        
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

