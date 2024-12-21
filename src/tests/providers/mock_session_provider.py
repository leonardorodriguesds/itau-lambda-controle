from src.app.provider.session_provider import SessionProvider


class TestSessionProvider(SessionProvider):
    """
    Substitui o SessionProvider de produção para usar 
    a sessão de teste (SQLite in-memory) ao invés de MySQL.
    """
    def __init__(self, session):
        self._session = session

    def get_session(self):
        return self._session

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def close(self):
        pass