from __future__ import annotations
import os, time, typing as t
import requests

CR_BASE = "https://api.crossref.org/works"

class CrossrefSource:
    def __init__(self, mailto: str | None = None, timeout: int = 30):
        self.mailto = mailto or os.getenv("CROSSREF_MAILTO")
        self.session = requests.Session()
        self.timeout = timeout

    def _get(self, params: dict) -> dict:
        if self.mailto:
            params = {**params, "mailto": self.mailto}
        r = self.session.get(CR_BASE, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def search(self, query: str, publisher: str, max_records: int = 300) -> list[dict]:
        rows, got, cursor = [], 0, "*"
        while got < max_records:
            params = {
                "query": query,
                "filter": f"publisher-name:{publisher}",
                "rows": min(100, max_records - got),
                "cursor": cursor,
                "cursor_max": min(1000, max_records),
                "select": "DOI,title,author,issued,container-title,link,abstract,publisher,license,type",
                "sort": "relevance",
            }
            data = self._get(params)
            items = data.get("message", {}).get("items", []) or []
            if not items:
                break
            rows.extend(items)
            got += len(items)
            cursor = data.get("message", {}).get("next-cursor")
            if not cursor:
                break
            time.sleep(0.2)
        return rows

def item_to_row(it: dict, source_label: str) -> dict:
    doi = it.get("DOI")
    title = (it.get("title") or [None])[0]
    authors = "; ".join([f"{a.get('given','')} {a.get('family','')}".strip() for a in it.get("author", [])]) or None
    year = None
    if it.get("issued", {}).get("date-parts"):
        year = it["issued"]["date-parts"][0][0]
    link_url = None
    for lk in it.get("link", []) or []:
        if lk.get("intended-application") == "text-mining":
            link_url = lk.get("URL")
            break
    if not link_url and it.get("link"):
        link_url = it["link"][0].get("URL")
    abstract = it.get("abstract")
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
        "abstract": abstract,
    }
