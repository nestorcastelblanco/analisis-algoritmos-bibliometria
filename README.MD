# Análisis de Algoritmos – Bibliometría

Pipeline end-to-end para **ingesta**, **deduplicación** y **carga** de artículos en PostgreSQL, con **UI en Streamlit** y (opcionalmente) API con FastAPI.  
Fuentes actuales: **Crossref** (ACM / SAGE) y, opcionalmente, **ScienceDirect** (Elsevier API).

## 1) Requisitos

- **Python** 3.10–3.12  
- **PostgreSQL** 14+ (recomendado 17.x) con extensión `pg_trgm`  
- **Git** (opcional, para clonar)  
- Internet (para Crossref / ScienceDirect)

## 2) Obtener el código

Coloca el repositorio en una ruta local, por ejemplo:

- Windows: `C:\analisis-algoritmos-bibliometria`  
- Linux/macOS: `~/analisis-algoritmos-bibliometria`

## 3) Variables de entorno (`.env`)

Crea un archivo **`.env`** en la **raíz** del proyecto:

```
APP_ENV=dev
APP_PORT=8000
UI_PORT=8501

# --- Postgres ---
POSTGRES_DB=biblio
POSTGRES_USER=postgres
POSTGRES_PASSWORD=TU_PASSWORD
POSTGRES_HOST=localhost
POSTGRES_PORT=5432          # usa 5433 si tu servidor corre allí

# Tiene prioridad sobre las variables anteriores:
DATABASE_URL=postgresql+psycopg://postgres:TU_PASSWORD@localhost:5432/biblio

# --- NLP ---
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
SPACY_MODEL=en_core_web_sm

# --- Crossref (recomendado: correo de contacto) ---
CROSSREF_MAILTO=tu_correo@tu-dominio.com

# --- ScienceDirect (Elsevier) - opcional ---
ELSEVIER_API_KEY=
ELSEVIER_INSTTOKEN=
```

> Si no tienes `ELSEVIER_API_KEY`, déjala vacía: la UI omitirá ScienceDirect automáticamente.

## 4) Entorno virtual + dependencias

### Windows (PowerShell)
```powershell
cd C:\analisis-algoritmos-bibliometria
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### Linux/macOS (bash)
```bash
cd ~/analisis-algoritmos-bibliometria
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Si no tienes `requirements.txt`, usa este contenido de referencia:

```
streamlit==1.38.0
fastapi==0.115.0
uvicorn==0.30.6
SQLAlchemy==2.0.36
psycopg[binary]==3.2.9
pandas==2.3.0
requests==2.32.4
python-dotenv==1.0.1
regex==2025.7.34
scikit-learn==1.5.2
spacy==3.8.7
sentence-transformers==3.0.1
```

> (Opcional) Descargar modelo de spaCy:  
> `python -m spacy download en_core_web_sm`

## 5) Base de datos y esquema

Crea la base **biblio** y habilita `pg_trgm`. Si tu repo trae SQLs en `db/init/`, ejecútalos ahora.

### Opción A — con `psql`
```sql
CREATE DATABASE biblio;
\c biblio;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

Esquema mínimo (si no usas `db/init/`):

```sql
CREATE TABLE IF NOT EXISTS paper (
  id BIGSERIAL PRIMARY KEY,
  title TEXT,
  doi TEXT UNIQUE,
  pii TEXT,
  authors TEXT,
  container_title TEXT,
  published TEXT,
  url TEXT
);

CREATE TABLE IF NOT EXISTS staging_papers (
  title TEXT,
  doi TEXT,
  pii TEXT,
  authors TEXT,
  container_title TEXT,
  published TEXT,
  source TEXT,
  url TEXT,
  abstract TEXT
);
```

### Opción B — sin `psql` (desde Python)
```bash
python - <<'PY'
from etl.db import get_engine
from sqlalchemy import text
schema = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TABLE IF NOT EXISTS paper (
  id BIGSERIAL PRIMARY KEY,
  title TEXT,
  doi TEXT UNIQUE,
  pii TEXT,
  authors TEXT,
  container_title TEXT,
  published TEXT,
  url TEXT
);
CREATE TABLE IF NOT EXISTS staging_papers (
  title TEXT,
  doi TEXT,
  pii TEXT,
  authors TEXT,
  container_title TEXT,
  published TEXT,
  source TEXT,
  url TEXT,
  abstract TEXT
);
"""
eng = get_engine()
with eng.begin() as con:
    for stmt in schema.split(";"):
        s = stmt.strip()
        if s:
            con.execute(text(s))
print("Esquema OK")
PY
```

## 6) Probar conexión a la BD

```bash
python - <<'PY'
from etl.db import get_engine
from sqlalchemy import text
print("DB OK" if get_engine().connect().execute(text("select 1")).scalar()==1 else "DB FAIL")
PY
```

## 7) Ejecutar la UI (Streamlit)

```bash
python -m streamlit run ui/app.py
```

En **“Ingesta automática”**:
1. Escribe una cadena (sin comillas), ej.: `information retrieval`.  
2. Selecciona **ACM** y/o **SAGE** (ScienceDirect solo si configuraste `ELSEVIER_API_KEY`).  
3. Click en **“Buscar y cargar”** → se descargan resultados, se deduplican y, si hay datos, se **genera un CSV** y se lanza el **ETL** hacia PostgreSQL.

### ETL manual (opcional)
```bash
# con un CSV específico mostrado por la UI
python etl/run_csv_ingest.py --input data/raw/combined_YYYYMMDD_HHMMSS.csv --source acm

# o usar el CSV más reciente
python etl/run_csv_ingest.py --input latest --source acm
```

## 8) Estructura del proyecto

```
analisis-algoritmos-bibliometria/
├─ etl/
│  ├─ source/
│  │  ├─ crossref_source.py      # Crossref (member + offset, UA con mailto)
│  │  └─ sciencedirect.py        # Elsevier API (opcional)
│  ├─ db.py                      # conexión SQLAlchemy (usa .env)
│  └─ run_csv_ingest.py          # CSV -> staging_papers -> (upsert) paper
├─ ui/
│  ├─ app.py                     # home Streamlit
│  └─ pages/
│     ├─ 1_Ingesta_automatica.py
│     └─ 2_DB_Status.py          # (opcional) estado BD
├─ db/init/                      # SQL canónicos (si aplica)
├─ data/
│  ├─ raw/                       # CSV combinados
│  └─ interim/                   # datos limpios/deduplicados
├─ scripts/                      # utilidades locales
├─ .env                          # ⚠️ local; no subir a git
├─ requirements.txt
└─ README.md
```

## 9) Problemas frecuentes (y soluciones)

- **`streamlit: comando no encontrado`**  
  Ejecuta con el venv: `python -m streamlit run ui/app.py`.

- **`ModuleNotFoundError: etl`**  
  Corre los comandos desde la **raíz** del repo. Los scripts usan “bootstrap” de ruta, y la UI llama con `cwd` correcto.

- **`HTTPError 400 Crossref`**  
  Asegúrate de tener `CROSSREF_MAILTO` en `.env`. El proyecto usa un cliente Crossref estable (filtro `member` + `offset`, sin `cursor_max/select/sort`).

- **`ValueError: Falta ELSEVIER_API_KEY`**  
  Deja vacía la key o desmarca ScienceDirect en la UI. Si tienes API key, colócala en `.env`.

- **No se genera CSV (`out_csv` vacío)**  
  Prueba otra cadena **sin comillas**. Empieza con **ACM** solo; si todo bien, añade **SAGE**.

- **`psql` no reconocido**  
  Usa pgAdmin/DBeaver o los scripts Python del repo para ejecutar SQL (ver pasos 5 y 6).

## 10) Buenas prácticas con Git

- Añade `.env` al `.gitignore` y comparte **.env.example** sin secretos.

**`.gitignore`** (sugerido):
```
.venv/
.env
data/raw/
__pycache__/
```

**`.env.example`** (plantilla para el equipo):
```
APP_ENV=dev
APP_PORT=8000
UI_PORT=8501
POSTGRES_DB=biblio
POSTGRES_USER=postgres
POSTGRES_PASSWORD=CHANGEME
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
DATABASE_URL=postgresql+psycopg://postgres:CHANGEME@localhost:5432/biblio
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
SPACY_MODEL=en_core_web_sm
CROSSREF_MAILTO=you@example.com
ELSEVIER_API_KEY=
ELSEVIER_INSTTOKEN=
```
