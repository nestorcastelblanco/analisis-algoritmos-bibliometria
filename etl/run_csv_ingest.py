# etl/run_csv_ingest.py
from __future__ import annotations
import argparse, sys, pandas as pd
from sqlalchemy import text, inspect
from etl.db import get_engine  # ya lo tienes

def ensure_staging(engine, df: pd.DataFrame, table="staging_papers"):
    # Crea o agrega (pandas crea la tabla si no existe)
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    return table

def paper_table_compatible(engine, df_cols) -> tuple[bool, list[str]]:
    insp = inspect(engine)
    if not insp.has_table("paper"):
        return False, []
    cols = {c["name"] for c in insp.get_columns("paper")}
    wanted = ["title","doi","pii","authors","container_title","published","source","url","abstract"]
    use = [c for c in wanted if c in cols]
    # Necesitamos al menos doi + title para un upsert sensato
    return (("doi" in cols) and (len(use) >= 2)), use

def upsert_into_paper(engine, staging_table, cols):
    # Inserta solo filas cuyo DOI no exista (si doi NULL, usa título normalizado)
    collist = ", ".join(cols)
    select_cols = ", ".join([f"s.{c}" for c in cols])
    with engine.begin() as conn:
        # Inserta por DOI no existente
        conn.execute(text(f"""
            INSERT INTO paper ({collist})
            SELECT {select_cols}
            FROM {staging_table} s
            LEFT JOIN paper p ON (p.doi = s.doi AND p.doi IS NOT NULL)
            WHERE (p.doi IS NULL) 
        """))
        # Opcional: insertar por título cuando no hay DOI (evita colisiones simples)
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
    # Asegura columnas esperadas aunque falten
    for c in ["title","doi","pii","authors","container_title","published","source","url","abstract"]:
        if c not in df.columns:
            df[c] = None

    # Cargar a staging
    staging = ensure_staging(engine, df)

    # Intentar upsert a tabla canónica `paper`
    ok, cols = paper_table_compatible(engine, df.columns)
    if ok:
        upsert_into_paper(engine, staging, cols)
        print(f"[OK] Cargado en {staging} y upsert en paper ({len(cols)} cols).")
    else:
        print(f"[OK] Cargado en {staging}. No se detectó tabla 'paper' compatible; puedes mergear luego.")

if __name__ == "__main__":
    sys.exit(main())
