# etl/db.py
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# etl -> repo root
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)

def get_engine() -> Engine:
    url = os.getenv("DATABASE_URL")
    if not url:
        # fallback por variables separadas si lo prefieres
        user = os.getenv("POSTGRES_USER", "postgres")
        pwd = os.getenv("POSTGRES_PASSWORD", "")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db   = os.getenv("POSTGRES_DB", "postgres")
        url = f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True, future=True)
