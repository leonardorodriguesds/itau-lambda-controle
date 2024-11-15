import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "lambdacontrole")

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    "?charset=utf8mb4"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"charset": "utf8mb4"},
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def set_charset(db):
    db.execute(text("SET NAMES utf8mb4;"))
    db.execute(text("SET CHARACTER SET utf8mb4;"))
    db.execute(text("SET character_set_connection=utf8mb4;"))

def get_session():
    db = SessionLocal()
    try:
        set_charset(db)
        yield db
    finally:
        db.close()
