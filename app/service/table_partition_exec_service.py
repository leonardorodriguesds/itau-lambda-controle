from sqlalchemy.orm import Session
from datetime import datetime
from models.table_partition_exec import TablePartitionExec
from models.dto.table_partition_exec_dto import PartitionDTO, TablePartitionExecDTO
from exceptions.table_insert_error import TableInsertError
from repositories.table_partition_exec_repository import TablePartitionExecRepository
from repositories.table_repository import TableRepository

class TablePartitionExecService:
    def __init__(self, session: Session):
        self.session = session
        self.repository = TablePartitionExecRepository(session)
        self.table_repo = TableRepository(session)
        
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

            for partition in resolved_partitions:
                latest_entry = self.repository.get_latest_by_table_partition(
                    table.id, partition.partition_id
                )
                if latest_entry:
                    latest_entry.tag_latest = False
                    latest_entry.deletion_date = datetime.utcnow()
                    latest_entry.deleted_by_user = dto.user

                new_entry = TablePartitionExec(
                    table_id=table.id,
                    partition_id=partition.partition_id,
                    value=partition.value,
                    execution_date=datetime.utcnow(),
                    tag_latest=True,
                )
                self.repository.save(new_entry)

            self.repository.commit()
            return {"message": "Table partition execution entries registered successfully."}

        except Exception as e:
            self.repository.rollback()
            raise TableInsertError(f"Erro ao registrar execuções de partições: {str(e)}")
