from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.db import get_engine
from sqlalchemy import inspect

eng = get_engine()
insp = inspect(eng)

for tbl in ["paper", "source", "staging_papers"]:
    try:
        cols = [c["name"] for c in insp.get_columns(tbl)]
        print(f"{tbl} -> {cols}")
    except Exception as e:
        print(f"{tbl} -> ERROR: {e}")
