# ui/pages/1_Ingesta_automatica.py
import os
import sys
from pathlib import Path

# Añade la raíz del repo al sys.path
ROOT = Path(__file__).resolve().parents[2]  # ui/pages -> ui -> (repo root)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Carga .env desde la raíz del proyecto
from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / ".env", override=False)

import streamlit as st
import subprocess
import sys as _sys

from etl.ingest_service import run_ingest


def has_elsevier_key() -> bool:
    """True si existe una API key válida para Elsevier."""
    val = (os.getenv("ELSEVIER_API_KEY") or "").strip().strip('"').strip("'")
    return bool(val) and val.lower() not in {"<tu_api_key>", "tu_api_key"}


st.set_page_config(page_title="Ingesta automatica")

st.title("Ingesta automatica de articulos")
st.caption("Ingresa la cadena, elige fuentes y ejecuta descarga -> unificacion/dedup -> ETL -> BD.")

query = st.text_input("Cadena de busqueda", value='"generative artificial intelligence"')

# Fuentes disponibles y defaults según si hay API key de Elsevier
options = ["acm", "sage", "sciencedirect"]
default_sources = ["acm", "sage"] + (["sciencedirect"] if has_elsevier_key() else [])
sources = st.multiselect("Fuentes", options, default=default_sources)

per_source = st.slider("Max. registros por fuente", min_value=50, max_value=1000, value=300, step=50)

if st.button("Buscar y cargar"):
    if not query.strip():
        st.error("Ingresa una cadena de busqueda")
        st.stop()

    # Degradación elegante: si no hay API key, quitar ScienceDirect
    if "sciencedirect" in sources and not has_elsevier_key():
        sources = [s for s in sources if s != "sciencedirect"]
        st.warning("No se encontro ELSEVIER_API_KEY en .env. Se omitira ScienceDirect para esta busqueda.")

    with st.spinner("Descargando y unificando..."):
        result = run_ingest(query, sources, per_source=per_source)

    st.success("Descarga completada")
    st.write(result)

    st.write("Ejecutando ETL -> BD...")
    try:
        proc = subprocess.run(
            [
                _sys.executable,
                "etl/run_csv_ingest.py",
                "--input", result["out_csv"],
                "--source", ",".join(sources),
            ],
            capture_output=True,
            text=True,
        )
        st.code(proc.stdout or "(sin salida)")
        if proc.returncode != 0:
            st.error(proc.stderr or "ETL finalizo con error")
        else:
            st.success("ETL finalizada y datos cargados en PostgreSQL")
    except Exception as e:
        st.exception(e)
