# etl/db.py
import os
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

# Cargar variables del .env en el entorno
load_dotenv()

def _build_db_url_from_env() -> Optional[str]:
    """Si no hay DATABASE_URL, construye una a partir de POSTGRES_*."""
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")
    if all([host, port, user, password, dbname]):
        # psycopg v3 (driver moderno)
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
    return None

# Prioridad: DATABASE_URL del .env -> construir con POSTGRES_* -> valor por defecto
DB_URL = (
    os.getenv("DATABASE_URL")
    or _build_db_url_from_env()
    or "postgresql+psycopg://postgres:NestoR2909@localhost:5433/biblio"  # <- ajusta si quieres
)

def get_engine(echo: Optional[bool] = None) -> Engine:
    """
    Crea el Engine de SQLAlchemy.
    - echo: si None, se activa sólo si APP_ENV=dev
    """
    if echo is None:
        echo = (os.getenv("APP_ENV", "dev").lower() == "dev")

    # pool_pre_ping evita conexiones muertas; future=True para API 2.x
    engine = create_engine(
        DB_URL,
        pool_pre_ping=True,
        future=True,
        echo=echo,
    )
    return engine

# Session factory opcional (útil si usas ORM más adelante)
SessionLocal = sessionmaker(bind=get_engine(echo=False), autoflush=False, autocommit=False, future=True)

# Prueba manual: python -m etl.db
if __name__ == "__main__":
    from sqlalchemy import text
    eng = get_engine()
    with eng.connect() as conn:
        val = conn.execute(text("SELECT 1")).scalar()
        print("✅ Conexión OK, SELECT 1 ->", val)
