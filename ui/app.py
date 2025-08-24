import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # <-- ui -> repo root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / ".env", override=False)

import streamlit as st

st.set_page_config(page_title="Bibliometria")
st.title("Bibliometria – Analitica de articulos")
st.write("Usa el menu lateral para ir a Ingesta automatica.")
