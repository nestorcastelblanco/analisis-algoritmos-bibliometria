from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.db import get_engine
from sqlalchemy import text, inspect

eng = get_engine()
insp = inspect(eng)

# Descubrir columnas de source
try:
    s_cols = {c["name"] for c in insp.get_columns("source")}
except Exception:
    s_cols = set()

# PK de source: id si existe, si no source_id, si no None
src_pk = "id" if "id" in s_cols else ("source_id" if "source_id" in s_cols else None)
# Columna legible: code si existe, si no name, si no NULL
src_disp = "code" if "code" in s_cols else ("name" if "name" in s_cols else None)

# Construir JOIN y expresión mostrable
if src_pk:
    join_clause = f"LEFT JOIN source s ON s.{src_pk} = p.source_id"
else:
    join_clause = "/* no join possible: source has no id/source_id */"

if src_disp:
    source_expr = f"COALESCE(s.{src_disp}, '?')"
else:
    source_expr = "NULL::text"

sql = f"""
SELECT p.title, p.doi, {source_expr} AS source, p.url
FROM paper p
{join_clause if src_pk else ""}
ORDER BY p.title DESC
LIMIT 5
"""

with eng.connect() as con:
    rows = con.execute(text(sql)).fetchall()
    for r in rows:
        print(r)

print("\\n-- SQL used --")
print(sql)
