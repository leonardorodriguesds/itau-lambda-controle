from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging

from models.table_execution import TableExecution

class TableExecutionRepository:
    def __init__(self, session, logger: logging.Logger):
        self.session = session
        self.logger = logger
        
    def get_latest_execution(self, table_id: int):
        """
        Retorna a última execução de uma tabela pelo ID.
        :param table_id: ID da tabela.
        :return: Instância de TableExecution ou None.
        """
        try:
            self.logger.debug(f"[TableExecutionRepository] get last execution for table [{table_id}]")
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

    def get_execution_by_id(self, execution_id):
        """
        Retorna uma execução pelo ID.
        :param execution_id: ID da execução.
        :return: Instância de TableExecution ou None.
        """
        try:
            return self.session.query(TableExecution).filter_by(id=execution_id).one_or_none()
        except SQLAlchemyError as e:
            self.logger.error(f"Erro ao buscar execução pelo ID {execution_id}: {str(e)}")
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

    def delete_execution(self, execution_id):
        """
        Exclui uma execução pelo ID.
        :param execution_id: ID da execução a ser excluída.
        """
        try:
            execution = self.get_execution_by_id(execution_id)
            if execution:
                self.session.delete(execution)
                self.session.commit()
                self.logger.info(f"Execução {execution_id} excluída com sucesso.")
            else:
                self.logger.warning(f"Execução {execution_id} não encontrada para exclusão.")
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Erro ao excluir execução {execution_id}: {str(e)}")
            raise
