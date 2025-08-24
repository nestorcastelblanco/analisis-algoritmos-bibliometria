# api/routers/ingest.py
from fastapi import APIRouter
from pydantic import BaseModel
from etl.ingest_service import run_ingest
import subprocess, sys


router = APIRouter(prefix="/ingest", tags=["ingest"])


class IngestReq(BaseModel):
query: str
sources: list[str] = ["sciencedirect", "acm", "sage"]
per_source: int = 300


@router.post("/")
def ingest(req: IngestReq):
result = run_ingest(req.query, req.sources, per_source=req.per_source)
# Dispara ETL
p = subprocess.run([sys.executable, "etl/run_csv_ingest.py", "--input", result["out_csv"], "--source", ",".join(req.sources)], capture_output=True, text=True)
return {"ingest": result, "etl_stdout": p.stdout, "etl_stderr": p.stderr, "returncode": p.returncode}