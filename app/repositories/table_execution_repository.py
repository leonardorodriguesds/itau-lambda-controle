from typing import Any, Dict
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging

from repositories.generic_repository import GenericRepository
from models.table_execution import TableExecution

class TableExecutionRepository(GenericRepository[TableExecution]):
    def __init__(self, session: Session, logger: logging.Logger):
        super().__init__(session, TableExecution, logger)
        self.session = session
        self.logger = logger
        
    def get_latest_execution(self, table_id: int):
        """
        Retorna a última execução de uma tabela pelo ID.
        :param table_id: ID da tabela.
        :return: Instância de TableExecution ou None.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] get last execution for table [{table_id}]")
            return self.session.query(TableExecution).filter_by(table_id=table_id).order_by(TableExecution.date_time.desc()).first()
        except SQLAlchemyError as e:
            self.logger.error(f"Erro ao buscar última execução da tabela [{table_id}]: {str(e)}")
            raise

    def create_execution(self, table_id: int, source: str):
        """
        Cria uma nova entrada na tabela TableExecution.
        :param table_id: ID da tabela associada à execução.
        :param source: Origem da execução.
        :return: A instância de TableExecution criada.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] create execution for table [{table_id}]")
            new_execution = TableExecution(
                table_id=table_id,
                source=source
            )
            self.session.add(new_execution)
            self.session.commit()
            self.logger.info(f"Nova execução criada: {new_execution.id}")
            return new_execution
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Erro ao criar execução: {str(e)}")
            raise

    def get_executions_by_table(self, table_id: int):
        """
        Retorna todas as execuções associadas a uma tabela.
        :param table_id: ID da tabela.
        :return: Lista de instâncias de TableExecution.
        """
        try:
            return self.session.query(TableExecution).filter_by(table_id=table_id).all()
        except SQLAlchemyError as e:
            self.logger.error(f"Erro ao buscar execuções pela tabela {table_id}: {str(e)}")
            raise

    def get_latest_execution_with_restrictions(self, table_id: int, required_partitions: Dict[str, Any]):
        """
        Consulta diretamente no banco de dados para encontrar a última execução que respeita as restrições de partições
        considerando todas as partições obrigatórias.

        :param table_id: ID da tabela.
        :param required_partitions: Dicionário com as partições obrigatórias e seus valores.
        :return: A última execução que respeita as restrições ou None.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Getting latest execution with restrictions for table [{table_id}]")

        available_partition_keys_query = text("""
            SELECT DISTINCT p.name
            FROM partitions p
            WHERE table_id = :table_id
        """)

        available_partition_keys = self.session.execute(available_partition_keys_query, {"table_id": table_id}).fetchall()
        available_partition_keys = [row[0] for row in available_partition_keys]

        filtered_partitions = {key: value for key, value in required_partitions.items() if key in available_partition_keys}

        if not filtered_partitions:
            self.logger.debug(f"[{self.__class__.__name__}] No overlapping partitions found for table [{table_id}]. Returning None.")
            return None

        self.logger.debug(f"[{self.__class__.__name__}] Overlapping partitions found for table [{table_id}]: {filtered_partitions}")

        partition_conditions = " AND ".join(
            [f"""
            EXISTS (
                SELECT 1 
                FROM table_partition_exec tp
                JOIN partitions p ON tp.partition_id = p.id
                WHERE tp.execution_id = te.id 
                AND p.name = :{key}_name 
                AND tp.value = :{key}_value
            )
            """ for key in filtered_partitions.keys()]
        )

        query = text(f"""
            SELECT te.*
            FROM table_execution te
            WHERE te.table_id = :table_id
            AND {partition_conditions}
            ORDER BY te.date_time DESC
            LIMIT 1
        """)

        params = {"table_id": table_id}
        for key, value in filtered_partitions.items():
            params[f"{key}_name"] = key
            params[f"{key}_value"] = str(value)  

        self.logger.debug(f"[{self.__class__.__name__}] Executing SQL query for latest execution with restrictions: {query}")
        
        result = self.session.execute(query, params).fetchone()

        if result:
            self.logger.debug(f"[{self.__class__.__name__}] Found latest execution with restrictions for table [{table_id}]: {result}")
            return TableExecution(**result._mapping)

        self.logger.debug(f"[{self.__class__.__name__}] No execution found with restrictions for table [{table_id}].")
        return None

