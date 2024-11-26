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
        Salva um objeto no banco de dados.

        :param obj: Objeto a ser salvo.
        :return: Objeto salvo.
        """
        if hasattr(obj, 'id') and obj.id:
            return self.update(obj.id, obj.__dict__)
        else:
            return self.add(obj)

    def get_by_id(self, obj_id: int) -> Optional[T]:
        """
        Obtém um objeto pelo ID, considerando date_deleted como null.

        :param obj_id: ID do objeto.
        :return: Objeto encontrado ou None.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Getting object by ID: [{obj_id}]")
        return self.db_session.query(self.model).filter(
            and_(self.model.id == obj_id, self.model.date_deleted.is_(None))
        ).first()

    def get_all(self) -> List[T]:
        """
        Retorna todos os objetos, considerando date_deleted como null.

        :return: Lista de objetos.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Getting all objects")
        return self.db_session.query(self.model).filter(
            self.model.date_deleted.is_(None)
        ).all()

    def add(self, obj: T) -> T:
        """
        Adiciona um novo objeto.

        :param obj: Objeto a ser adicionado.
        :return: Objeto adicionado.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Adding object: [{obj.__dict__}]")
        self.db_session.add(obj)
        self.db_session.commit()
        self.db_session.refresh(obj)
        return obj

    def update(self, obj_id: int, updated_data: dict) -> Optional[T]:
        """
        Atualiza um objeto pelo ID, considerando date_deleted como null.

        :param obj_id: ID do objeto a ser atualizado.
        :param updated_data: Dados atualizados em formato de dicionário.
        :return: Objeto atualizado ou None.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Updating object with ID [{obj_id}]: [{updated_data}]")
        obj = self.get_by_id(obj_id)
        if not obj:
            return None
        for key, value in updated_data.items():
            setattr(obj, key, value)
        self.db_session.commit()
        self.db_session.refresh(obj)
        return obj

    def hard_delete(self, obj_id: int) -> bool:
        """
        Remove um objeto pelo ID.

        :param obj_id: ID do objeto a ser removido.
        :return: True se foi removido, False caso contrário.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Hard deleting object with ID [{obj_id}]")
        obj = self.get_by_id(obj_id)
        if not obj:
            return False
        self.db_session.delete(obj)
        self.db_session.commit()
        return True

    def soft_delete(self, obj_id: int) -> bool:
        """
        Marca um objeto como deletado (soft delete).

        :param obj_id: ID do objeto a ser marcado.
        :return: True se foi marcado, False caso contrário.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Soft deleting object with ID [{obj_id}]")
        obj = self.get_by_id(obj_id)
        if not obj:
            return False
        obj.date_deleted = datetime.now()
        self.db_session.commit()
        return True