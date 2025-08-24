# etl/source/crossref_source.py
from __future__ import annotations

import time
from typing import List, Dict, Any, Optional
from pathlib import Path
import os
import re
import requests
from dotenv import load_dotenv

# etl/source -> repo root
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env", override=False)

CR_BASE = "https://api.crossref.org/v1/works"   # version explícita
CR_MEMBERS = "https://api.crossref.org/v1/members"

def _user_agent() -> str:
    app = os.getenv("APP_ENV", "dev")
    mail = (os.getenv("CROSSREF_MAILTO") or "").strip()
    # Crossref pide contacto en UA o en mailto param
    if mail:
        return f"analisis-bibliometria/1.0 ({mail}); env={app}"
    return "analisis-bibliometria/1.0 (mailto:contact@example.com); env={}".format(app)

class CrossrefSource:
    def __init__(self, timeout: int = 30):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": _user_agent(),
            "Accept": "application/json",
        })
        self.timeout = timeout
        self.mailto = (os.getenv("CROSSREF_MAILTO") or "").strip()  # opcional

    def _get(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        # añadimos mailto como parámetro por cortesía
        if self.mailto:
            params = dict(params)
            params["mailto"] = self.mailto
        r = self.session.get(url, params=params, timeout=self.timeout)
        # Consejo: cuando dé 400, es útil ver el cuerpo
        if r.status_code >= 400:
            try:
                txt = r.text[:500]
            except Exception:
                txt = "<sin texto>"
            raise requests.HTTPError(f"{r.status_code} for {r.url} :: {txt}", response=r)
        return r.json()

    # --------- resolución del member id del editor ----------
    def _resolve_member_id(self, publisher: str) -> Optional[int]:
        params = {"query": publisher, "rows": 20}
        data = self._get(CR_MEMBERS, params)
        items = data.get("message", {}).get("items") or []
        if not items:
            return None
        # Heurística simple: mejor coincidencia por inclusión/Fuzzy light
        pub_norm = re.sub(r"\s+", " ", publisher).strip().lower()
        best_id, best_score = None, -1
        for it in items:
            name = (it.get("primary-name") or it.get("name") or "").lower()
            score = 0
            if pub_norm == name:
                score = 100
            elif pub_norm in name or name in pub_norm:
                score = 80
            else:
                # puntos por tokens compartidos
                toks = set(pub_norm.split())
                score = len(toks.intersection(name.split()))
            if score > best_score:
                best_score, best_id = score, it.get("id")
        return int(best_id) if best_id is not None else None

    # --------- búsqueda con paginación por offset ----------
    def search(self, query: str, publisher: str, max_records: int = 300) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        # resolver member id y usar filter=member:<id> (más robusto que publisher-name)
        member_id = None
        try:
            member_id = self._resolve_member_id(publisher)
        except requests.HTTPError:
            # si falla el endpoint de members, seguimos sin ID
            member_id = None

        got, offset = 0, 0
        while got < max_records:
            page_size = min(100, max_records - got)
            filters = f"member:{member_id}" if member_id else f"publisher-name:{publisher}"
            params = {
                "query.bibliographic": query,
                "filter": filters,
                "rows": page_size,
                "offset": offset,
            }
            data = self._get(CR_BASE, params)
            items = data.get("message", {}).get("items") or []
            if not items:
                break
            rows.extend(items)
            got += len(items)
            offset += len(items)
            time.sleep(0.15)  # ser amable
        return rows

def item_to_row(it: Dict[str, Any], source_label: str) -> Dict[str, Any]:
    doi = it.get("DOI")
    title = (it.get("title") or [None])[0]
    authors = "; ".join([f"{a.get('given','')} {a.get('family','')}".strip()
                         for a in it.get("author", [])]) or None
    year = None
    if it.get("issued", {}).get("date-parts"):
        year = it["issued"]["date-parts"][0][0]

    # URL de texto-mining si existe
    link_url = None
    for lk in it.get("link", []) or []:
        if lk.get("intended-application") == "text-mining":
            link_url = lk.get("URL"); break
    if not link_url and it.get("link"):
        link_url = it["link"][0].get("URL")

    return {
        "source": source_label,
        "title": title,
        "doi": doi,
        "pii": None,
        "authors": authors,
        "container_title": (it.get("container-title") or [None])[0],
        "published": str(year) if year else None,
        "openaccess": None,
        "url": link_url,
        "abstract": it.get("abstract"),
    }
