from logging import Logger
from typing import List
from sqlalchemy.orm import Session
from datetime import datetime
from repositories.table_execution_repository import TableExecutionRepository
from models.table_execution import TableExecution
from models.tables import Tables
from models.table_partition_exec import TablePartitionExec
from models.dto.table_partition_exec_dto import PartitionDTO, TablePartitionExecDTO
from exceptions.table_insert_error import TableInsertError
from repositories.table_partition_exec_repository import TablePartitionExecRepository
from repositories.table_repository import TableRepository

logger = Logger(__name__)

class TablePartitionExecService:
    def __init__(self, session: Session):
        self.session = session
        self.repository = TablePartitionExecRepository(session)
        self.table_repo = TableRepository(session)
        self.table_execution_repository = TableExecutionRepository(session)
        
    def trigger_tables(self, dto: TablePartitionExecDTO):
        logger.debug(f"[TablePartitionExecService] Triggering tables for: {dto}")
        tables: List[Tables] = self.table_repo.get_by_dependecy(dto.table_id)
        
        for table in tables:
            for task in table.task_table:
                pass
        
    def register_multiple_events(self, dtos: list[TablePartitionExecDTO]):
        """
        Registra múltiplos eventos de execução de partições para tabelas.
        """
        try:
            for dto in dtos:
                self.register_partitions_exec(dto)

            self.repository.commit()
            return {"message": f"{len(dtos)} eventos registrados com sucesso."}

        except Exception as e:
            self.repository.rollback()
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
        try:
            table = None
            if dto.table_id:
                table = self.table_repo.get_by_id(dto.table_id)
            elif dto.table_name:
                table = self.table_repo.get_by_name(dto.table_name)

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

            new_execution = self.table_execution_repository.create_execution(table.id, dto.source)

            for partition in resolved_partitions:
                existing_entry = self.repository.get_by_table_partition_and_value(
                    table_id=table.id,
                    partition_id=partition.partition_id,
                    value=partition.value,
                )
                if existing_entry:
                    logger.info(
                        f"Entrada já existente para table_id={table.id}, "
                        f"partition_id={partition.partition_id}, value={partition.value}."
                    )
                    continue  

                self.session.query(TablePartitionExec).filter_by(
                    table_id=table.id, partition_id=partition.partition_id
                ).update({"tag_latest": False})

                new_entry = TablePartitionExec(
                    table_id=table.id,
                    partition_id=partition.partition_id,
                    value=partition.value,
                    execution_date=datetime.utcnow(),
                    tag_latest=True,
                    execution_id=new_execution.id  
            )
            self.repository.save(new_entry)

            self.repository.commit()
            self.trigger_tables(dto)
            return {"message": "Table partition execution entries registered successfully."}

        except Exception as e:
            self.repository.rollback()
            raise TableInsertError(f"Erro ao registrar execuções de partições: {str(e)}")

