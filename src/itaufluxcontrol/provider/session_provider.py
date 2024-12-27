from injector import singleton, inject

from src.itaufluxcontrol.provider.database_provider import DatabaseProvider

@singleton
class SessionProvider:
    """Provedor de sessões Singleton para injeção."""
    
    @inject
    def __init__(self, database_service: DatabaseProvider):
        """
        Inicializa o SessionProvider com uma instância do DatabaseProvider.
        """
        self._database_service = database_service
        self._session_generator = self._database_service.get_session()
        self._session = next(self._session_generator)

    def get_session(self):
        """
        Retorna a sessão atual.
        """
        return self._session

    def commit(self):
        """
        Realiza o commit da sessão atual.
        """
        self._session.commit()

    def rollback(self):
        """
        Realiza o rollback da sessão atual.
        """
        self._session.rollback()

    def close(self):
        """
        Fecha o generator de sessão.
        """
        self._session_generator.close()
