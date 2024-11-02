import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "lambdacontrole")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_session():
    """
    Função para obter uma nova sessão do banco de dados.
    Exemplo de uso:
        with get_session() as session:
            # Operações de banco de dados
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
