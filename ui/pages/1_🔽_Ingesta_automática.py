# ui/pages/1_🔽_Ingesta_automática.py
import streamlit as st
import subprocess, sys
from etl.ingest_service import run_ingest


st.set_page_config(page_title="Ingesta automática", page_icon="🔽")


st.title("🔽 Ingesta automática de artículos")
st.caption("Ingresa la cadena de búsqueda, elige fuentes y ejecuta descarga→unificación→ETL→BD.")


query = st.text_input("Cadena de búsqueda", value='"generative artificial intelligence"')
sources = st.multiselect("Fuentes", ["sciencedirect", "sage", "acm"], default=["sciencedirect", "acm", "sage"])
per_source = st.slider("Máx. registros por fuente", 50, 1000, 300, step=50)


do_it = st.button("🚀 Buscar y cargar")


if do_it:
if not query.strip():
st.error("Ingresa una cadena de búsqueda")
st.stop()
with st.spinner("Descargando y unificando..."):
result = run_ingest(query, sources, per_source=per_source)
st.success("Descarga completada")
st.write(result)


# Lanza tu pipeline de ETL ya existente para cargar a BD
st.write("Ejecutando ETL → BD...")
try:
# Ajusta el nombre de tu script principal según tu repo
p = subprocess.run([sys.executable, "etl/run_csv_ingest.py", "--input", result["out_csv"], "--source", ",".join(sources)], capture_output=True, text=True)
st.code(p.stdout)
if p.returncode != 0:
st.error(p.stderr)
else:
st.success("ETL finalizada y datos cargados en PostgreSQL")
except Exception as e:
st.exception(e)