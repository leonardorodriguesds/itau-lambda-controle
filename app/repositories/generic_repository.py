from datetime import datetime
from logging import Logger
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_
from typing import Type, TypeVar, Generic, List, Optional

T = TypeVar('T')

class GenericRepository(Generic[T]):
    """
    Classe genérica de repositório para operações CRUD.
    """
    def __init__(self, db_session: Session, model: Type[T], logger: Logger):
        """
        Inicializa o repositório genérico.

        :param db_session: Sessão do banco de dados.
        :param model: Modelo da tabela associada.
        :param logger: Logger para logging de operações.
        """
        self.db_session = db_session
        self.model = model
        self.logger = logger

    def save(self, obj: T) -> T:
        """
        Salva ou atualiza um objeto no banco de dados.

        :param obj: Objeto a ser salvo.
        :return: Objeto salvo.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Saving object: [{obj.__dict__}]")
            obj = self.db_session.merge(obj)  
            self.db_session.commit()
            self.db_session.refresh(obj)
            return obj
        except Exception as e:
            self.logger.error(f"Error saving object: {e}")
            self.db_session.rollback()
            raise

    def get_by_id(self, obj_id: int) -> Optional[T]:
        """
        Obtém um objeto pelo ID, considerando `date_deleted` como null.

        :param obj_id: ID do objeto.
        :return: Objeto encontrado ou None.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Getting object by ID: [{obj_id}]")
            return self.db_session.query(self.model).filter(
                and_(self.model.id == obj_id, self.model.date_deleted.is_(None))
            ).first()
        except Exception as e:
            self.logger.error(f"Error fetching object by ID [{obj_id}]: {e}")
            raise

    def get_all(self) -> List[T]:
        """
        Retorna todos os objetos, considerando `date_deleted` como null.

        :return: Lista de objetos.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Getting all objects")
            return self.db_session.query(self.model).filter(
                self.model.date_deleted.is_(None)
            ).all()
        except Exception as e:
            self.logger.error(f"Error fetching all objects: {e}")
            raise

    def add(self, obj: T) -> T:
        """
        Adiciona um novo objeto ao banco de dados.

        :param obj: Objeto a ser adicionado.
        :return: Objeto adicionado.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Adding object: [{obj.__dict__}]")
            self.db_session.add(obj)
            self.db_session.commit()
            self.db_session.refresh(obj)
            return obj
        except Exception as e:
            self.logger.error(f"Error adding object: {e}")
            self.db_session.rollback()
            raise

    def update(self, obj_id: int, updated_data: dict) -> Optional[T]:
        """
        Atualiza um objeto pelo ID.

        :param obj_id: ID do objeto a ser atualizado.
        :param updated_data: Dados atualizados em formato de dicionário.
        :return: Objeto atualizado ou None.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Updating object with ID [{obj_id}]: [{updated_data}]")
            obj = self.get_by_id(obj_id)
            if not obj:
                self.logger.warning(f"Object with ID [{obj_id}] not found for update.")
                return None

            obj = self.db_session.merge(obj)

            for key, value in updated_data.items():
                setattr(obj, key, value)
            self.db_session.commit()
            self.db_session.refresh(obj)
            return obj
        except Exception as e:
            self.logger.error(f"Error updating object with ID [{obj_id}]: {e}")
            self.db_session.rollback()
            raise

    def hard_delete(self, obj_id: int) -> bool:
        """
        Remove permanentemente um objeto pelo ID.

        :param obj_id: ID do objeto a ser removido.
        :return: True se foi removido, False caso contrário.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Hard deleting object with ID [{obj_id}]")
            obj = self.get_by_id(obj_id)
            if not obj:
                self.logger.warning(f"Object with ID [{obj_id}] not found for hard delete.")
                return False
            self.db_session.delete(obj)
            self.db_session.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error hard deleting object with ID [{obj_id}]: {e}")
            self.db_session.rollback()
            raise

    def soft_delete(self, obj_id: int) -> bool:
        """
        Marca um objeto como deletado (soft delete).

        :param obj_id: ID do objeto a ser marcado.
        :return: True se foi marcado, False caso contrário.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Soft deleting object with ID [{obj_id}]")
            obj = self.get_by_id(obj_id)
            if not obj:
                self.logger.warning(f"Object with ID [{obj_id}] not found for soft delete.")
                return False

            obj = self.db_session.merge(obj)

            obj.date_deleted = datetime.now()
            self.db_session.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error soft deleting object with ID [{obj_id}]: {e}")
            self.db_session.rollback()
            raise
