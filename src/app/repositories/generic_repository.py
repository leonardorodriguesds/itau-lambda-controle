from datetime import datetime
from logging import Logger
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_
from typing import Type, TypeVar, Generic, List, Optional

T = TypeVar('T')

class GenericRepository(Generic[T]):
    """
    Repositório genérico para operações CRUD.
    O controle de commit e rollback deve ser gerenciado externamente.
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
            if not obj.id:  
                self.db_session.add(obj)  
            else: 
                obj = self.db_session.merge(obj)  
            self.db_session.flush()  
            self.logger.debug(f"[{self.__class__.__name__}] Object ID after flush: {obj.id}")
            return obj
        except Exception as e:
            self.logger.error(f"Error saving object: {e}")
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
            self.logger.error(f"[{self.__class__.__name__}] Error fetching object by ID [{obj_id}]: {e}")
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
            self.logger.error(f"[{self.__class__.__name__}] Error fetching all objects: {e}")
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
                self.logger.warning(f"[{self.__class__.__name__}] Object with ID [{obj_id}] not found for update.")
                return None

            for key, value in updated_data.items():
                setattr(obj, key, value)
            obj = self.db_session.merge(obj)  
            self.db_session.flush()
            return obj
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error updating object with ID [{obj_id}]: {e}")
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
                self.logger.warning(f"[{self.__class__.__name__}] Object with ID [{obj_id}] not found for hard delete.")
                return False
            self.db_session.delete(obj) 
            return True
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error hard deleting object with ID [{obj_id}]: {e}")
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
                self.logger.warning(f"[{self.__class__.__name__}] Object with ID [{obj_id}] not found for soft delete.")
                return False

            obj.date_deleted = datetime.now()
            obj = self.db_session.merge(obj) 
            self.db_session.flush()
            return True
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error soft deleting object with ID [{obj_id}]: {e}")
            raise

    def query(self, **filters) -> List[T]: 
        """
        Consulta objetos no banco de dados com base em filtros dinâmicos.

        :param filters: Dicionário de filtros para consulta.
        :return: Lista de objetos que atendem aos filtros.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Querying objects with filters: {filters}")
            query = self.db_session.query(self.model).filter_by(date_deleted=None)
            for attr, value in filters.items():
                if '.' in attr:
                    relation, column = attr.split('.')
                    relationship_attr = getattr(self.model, relation, None)
                    if not relationship_attr:
                        raise AttributeError(f"Relacionamento '{relation}' não encontrado no modelo '{self.model.__name__}'.")
                    
                    related_model = relationship_attr.property.mapper.class_
                    
                    query = query.join(relationship_attr).filter(getattr(related_model, column) == value)
                else:
                    query = query.filter(getattr(self.model, attr) == value)
            return query.all()
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error querying objects: {e}")
            raise


    def flush(self):
        """
        Executa um flush na sessão do banco de dados.
        """
        try:
            self.logger.debug(f"[{self.__class__.__name__}] Flushing database session")
            self.db_session.flush()
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Error flushing database session: {e}")
            raise