# api/main.py (fragmento)
from fastapi import FastAPI
from api.routers import ingest as ingest_router


app = FastAPI(title="Analisis de Algoritmos – API")
app.include_router(ingest_router.router)