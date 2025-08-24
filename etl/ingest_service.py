from __future__ import annotations

import os
import csv
import json
import re
from typing import Literal, Iterator, Dict, Any, Tuple, List
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
# Carga .env desde la raiz del repo (etl -> repo root)
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

def normalize_title(s: str | None) -> str | None:
    if not s:
        return s
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9\s:;,\.\-\(\)\[\]{}]", "", s)
    return s

def dedupe_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    seen_keys = set()
    deduped: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    for r in rows:
        doi = (r.get("doi") or "").lower().strip()
        title_key = normalize_title(r.get("title")) or ""
        key = None
        if doi:
            key = ("doi", doi)
        elif title_key:
            key = ("title", title_key)
        if key and key in seen_keys:
            removed.append(r)
            continue
        if key:
            seen_keys.add(key)
        deduped.append(r)
    return deduped, removed

def run_ingest(query: str, sources: List[SourceOpt], per_source: int = 300) -> Dict[str, Any]:
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    all_rows: List[Dict[str, Any]] = []

    # ScienceDirect solo si hay API key
    if "sciencedirect" in sources:
        if _has_elsevier_key():
            cli = ScienceDirectClient()
            sd_rows = [entry_to_row(e) for e in iter_search(cli, query=query, max_records=per_source)]
            all_rows.extend(sd_rows)
        else:
            print("[WARN] ELSEVIER_API_KEY ausente o placeholder. Omitiendo ScienceDirect.")

    # Crossref para SAGE y ACM
    cr = CrossrefSource()
    if "sage" in sources:
        items = cr.search(query=query, publisher=PUBLISHER_MAP["sage"], max_records=per_source)
        all_rows.extend([item_to_row(it, "sage") for it in items])
    if "acm" in sources:
        items = cr.search(query=query, publisher=PUBLISHER_MAP["acm"], max_records=per_source)
        all_rows.extend([item_to_row(it, "acm") for it in items])

    deduped, removed = dedupe_rows(all_rows)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = str(Path(RAW_DIR) / f"combined_{stamp}.csv")
    log_json = str(Path(RAW_DIR) / f"dedupe_removed_{stamp}.json")

    if deduped:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(deduped[0].keys()))
            w.writeheader()
            w.writerows(deduped)
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
    }
