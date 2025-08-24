# etl/run_csv_ingest.py
from __future__ import annotations

# --- bootstrap de ruta para importar 'etl.*' aunque el CWD cambie ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]  # .../analisis-algoritmos-bibliometria
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# --------------------------------------------------------------------

import argparse
import pandas as pd
from sqlalchemy import text, inspect
from etl.db import get_engine  # requiere etl/db.py existente

def ensure_staging(engine, df: pd.DataFrame, table="staging_papers"):
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    return table

def paper_table_compatible(engine, df_cols) -> tuple[bool, list[str]]:
    insp = inspect(engine)
    if not insp.has_table("paper"):
        return False, []
    cols = {c["name"] for c in insp.get_columns("paper")}
    wanted = ["title","doi","pii","authors","container_title","published","source","url","abstract"]
    use = [c for c in wanted if c in cols]
    return (("doi" in cols) and (len(use) >= 2)), use

def upsert_into_paper(engine, staging_table, cols):
    collist = ", ".join(cols)
    select_cols = ", ".join([f"s.{c}" for c in cols])
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO paper ({collist})
            SELECT {select_cols}
            FROM {staging_table} s
            LEFT JOIN paper p ON (p.doi = s.doi AND p.doi IS NOT NULL)
            WHERE (p.doi IS NULL)
        """))
        if "title" in cols:
            conn.execute(text(f"""
                INSERT INTO paper ({collist})
                SELECT {select_cols}
                FROM {staging_table} s
                WHERE s.doi IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM paper p2
                      WHERE lower(regexp_replace(p2.title,'\\s+',' ','g')) =
                            lower(regexp_replace(s.title,'\\s+',' ','g'))
                  )
            """))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Ruta al CSV combinado")
    ap.add_argument("--source", default="", help="Etiqueta(s) de fuente (solo logging)")
    args = ap.parse_args()

    engine = get_engine()
    df = pd.read_csv(args.input)

    for c in ["title","doi","pii","authors","container_title","published","source","url","abstract"]:
        if c not in df.columns:
            df[c] = None

    staging = ensure_staging(engine, df)
    ok, cols = paper_table_compatible(engine, df.columns)
    if ok:
        upsert_into_paper(engine, staging, cols)
        print(f"[OK] Cargado en {staging} y upsert en paper ({len(cols)} cols).")
    else:
        print(f"[OK] Cargado en {staging}. No se detecto tabla 'paper' compatible; puedes mergear luego.")

if __name__ == "__main__":
    raise SystemExit(main())
