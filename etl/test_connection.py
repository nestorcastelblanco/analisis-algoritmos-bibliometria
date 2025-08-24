# etl/test_connection.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar las variables del archivo .env
load_dotenv()

# Leer DATABASE_URL desde .env (o usar valor por defecto si no existe)
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:NestoR2909@localhost:5433/biblio",
)

def main():
    try:
        engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            print("✅ Conexión exitosa a PostgreSQL, resultado:", result)
    except Exception as e:
        print("❌ Error al conectar:", e)

if __name__ == "__main__":
    main()
