# etl/ingest_service.py
from __future__ import annotations

import os, csv, json, re
from typing import Literal, Dict, Any, List, Tuple
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)

from etl.source.sciencedirect import ScienceDirectClient, iter_search, entry_to_row
from etl.source.crossref_source import CrossrefSource, item_to_row

RAW_DIR = "data/raw"

SourceOpt = Literal["sciencedirect", "sage", "acm"]
PUBLISHER_MAP = {
    "sage": "SAGE Publications",
    "acm": "Association for Computing Machinery",
}

def _has_elsevier_key() -> bool:
    v = (os.getenv("ELSEVIER_API_KEY") or "").strip().strip('"').strip("'")
    return bool(v) and v.lower() not in {"<tu_api_key>", "tu_api_key"}

def _strip_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def normalize_title(s: str | None) -> str | None:
    if not s: return s
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9\s:;,\.\-\(\)\[\]{}]", "", s)
    return s

def dedupe_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    seen = set(); deduped=[]; removed=[]
    for r in rows:
        doi = (r.get("doi") or "").lower().strip()
        title_key = normalize_title(r.get("title")) or ""
        key = ("doi", doi) if doi else ("title", title_key) if title_key else None
        if key and key in seen: removed.append(r); continue
        if key: seen.add(key)
        deduped.append(r)
    return deduped, removed

def run_ingest(query: str, sources: List[SourceOpt], per_source: int = 300) -> Dict[str, Any]:
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    all_rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    clean_query = _strip_quotes(query)

    if "sciencedirect" in sources:
        if _has_elsevier_key():
            try:
                cli = ScienceDirectClient()
                sd_rows = [entry_to_row(e) for e in iter_search(cli, query=clean_query, max_records=per_source)]
                all_rows.extend(sd_rows)
            except Exception as e:
                errors.append(f"ScienceDirect: {type(e).__name__}: {e}")
        else:
            errors.append("ScienceDirect omitido: ELSEVIER_API_KEY ausente/placeholder.")

    cr = CrossrefSource()
    if "sage" in sources:
        try:
            items = cr.search(query=clean_query, publisher=PUBLISHER_MAP["sage"], max_records=per_source)
            all_rows.extend([item_to_row(it, "sage") for it in items])
        except Exception as e:
            errors.append(f"SAGE/Crossref: {type(e).__name__}: {e}")

    if "acm" in sources:
        try:
            items = cr.search(query=clean_query, publisher=PUBLISHER_MAP["acm"], max_records=per_source)
            all_rows.extend([item_to_row(it, "acm") for it in items])
        except Exception as e:
            errors.append(f"ACM/Crossref: {type(e).__name__}: {e}")

    deduped, removed = dedupe_rows(all_rows)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = str(Path(RAW_DIR) / f"combined_{stamp}.csv")
    log_json = str(Path(RAW_DIR) / f"dedupe_removed_{stamp}.json")

    if deduped:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(deduped[0].keys()))
            w.writeheader(); w.writerows(deduped)
    else:
        out_csv = ""  # <- NO hay CSV

    with open(log_json, "w", encoding="utf-8") as f:
        json.dump(removed, f, ensure_ascii=False, indent=2)

    return {
        "query": query,
        "sources": sources,
        "total_raw": len(all_rows),
        "total_after_dedupe": len(deduped),
        "duplicates_removed": len(removed),
        "out_csv": out_csv,
        "log_json": log_json,
        "errors": errors,
    }
