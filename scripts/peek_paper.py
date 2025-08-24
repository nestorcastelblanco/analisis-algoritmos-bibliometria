from __future__ import annotations
import sys
from pathlib import Path

# Añade la raíz del repo al sys.path
ROOT = Path(__file__).resolve().parents[1]  # ...\analisis-algoritmos-bibliometria
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.db import get_engine
from sqlalchemy import text

eng = get_engine()
with eng.connect() as con:
    rows = con.execute(text("""
        SELECT p.title, p.doi, COALESCE(s.code,'?') AS source, p.url
        FROM paper p
        LEFT JOIN source s ON s.id = p.source_id
        ORDER BY p.title DESC
        LIMIT 5
    """)).fetchall()
    for r in rows:
        print(r)
