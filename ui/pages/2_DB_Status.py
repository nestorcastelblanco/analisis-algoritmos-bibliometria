# ui/pages/2_DB_Status.py
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # ui/pages -> ui -> root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / ".env", override=False)

import streamlit as st
from sqlalchemy import text, inspect
from etl.db import get_engine

st.set_page_config(page_title="DB Status")

st.title("Estado de la base de datos")

eng = get_engine()
insp = inspect(eng)

# Conteos
with eng.connect() as con:
    def q(sql: str):
        try:
            return con.execute(text(sql)).fetchall()
        except Exception as e:
            return [("ERROR", str(e))]

    st.subheader("Conteos")
    st.write({"staging_papers": q("SELECT count(*) FROM staging_papers")[0][0]})
    st.write({"paper": q("SELECT count(*) FROM paper")[0][0]})

# Peek con JOIN autoajustable
st.subheader("Muestra de 5 filas")
with eng.connect() as con:
    try:
        s_cols = {c["name"] for c in insp.get_columns("source")}
    except Exception:
        s_cols = set()

    src_pk = "id" if "id" in s_cols else ("source_id" if "source_id" in s_cols else None)
    src_disp = "code" if "code" in s_cols else ("name" if "name" in s_cols else None)

    if src_pk:
        join_clause = f"LEFT JOIN source s ON s.{src_pk} = p.source_id"
    else:
        join_clause = ""

    source_expr = f"COALESCE(s.{src_disp}, '?')" if src_disp else "NULL::text"

    sql = f"""
    SELECT p.title, p.doi, {source_expr} AS source, p.url
    FROM paper p
    {join_clause}
    ORDER BY p.title DESC
    LIMIT 5
    """

    try:
        rows = con.execute(text(sql)).fetchall()
        st.table(rows)
    except Exception as e:
        st.error(f"Error ejecutando peek: {e}")
        st.code(sql)
