from injector import singleton

from src.app.service.database import get_session

@singleton
class SessionProvider:
    """Provedor de sessões Singleton para injeção."""
    def __init__(self):
        self._session_generator = get_session()
        self._session = next(self._session_generator)

    def get_session(self):
        return self._session

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def close(self):
        self._session_generator.close()
