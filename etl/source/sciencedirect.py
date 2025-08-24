from __future__ import annotations

import os
import time
from typing import Iterator, Dict, Any
from pathlib import Path

from dotenv import load_dotenv
# Carga .env desde la raiz del repo (etl/source -> repo root)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env", override=False)

import requests
from urllib.parse import urlencode

SD_SEARCH_URL = "https://api.elsevier.com/content/search/sciencedirect"
SD_ARTICLE_PII_URL = "https://api.elsevier.com/content/article/pii/{}"

class ScienceDirectClient:
    def __init__(self, api_key: str | None = None, insttoken: str | None = None, timeout: int = 30):
        self.api_key = api_key or os.getenv("ELSEVIER_API_KEY")
        self.insttoken = insttoken or os.getenv("ELSEVIER_INSTTOKEN")
        if not self.api_key:
            raise ValueError("Falta ELSEVIER_API_KEY en .env")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "X-ELS-APIKey": self.api_key,
        })
        if self.insttoken:
            self.session.headers["X-ELS-Insttoken"] = self.insttoken
        self.timeout = timeout

    def search(self, query: str, count: int = 100, start: int = 0) -> Dict[str, Any]:
        params = {"query": query, "count": min(count, 100), "start": start}
        url = f"{SD_SEARCH_URL}?{urlencode(params)}"
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def fetch_article_meta(self, pii: str, view: str = "META_ABS") -> Dict[str, Any]:
        url = SD_ARTICLE_PII_URL.format(pii)
        r = self.session.get(url, params={"view": view}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

def iter_search(client: ScienceDirectClient, query: str, max_records: int = 300) -> Iterator[Dict[str, Any]]:
    got = 0
    start = 0
    page = 100
    while got < max_records:
        data = client.search(query=query, count=page, start=start)
        entries = (data.get("search-results", {}).get("entry") or [])
        if not entries:
            break
        for e in entries:
            yield e
        got += len(entries)
        start += len(entries)
        time.sleep(0.35)  # rate limit courtesy

def entry_to_row(e: Dict[str, Any]) -> Dict[str, Any]:
    links = {l.get("@ref"): l.get("@href") for l in (e.get("link") or [])}
    authors = None
    authors_obj = e.get("authors", {})
    if isinstance(authors_obj, dict):
        auth_list = authors_obj.get("author") or []
        if isinstance(auth_list, list):
            authors = "; ".join([a.get("authname", "") for a in auth_list if isinstance(a, dict)])
    return {
        "source": "sciencedirect",
        "title": e.get("dc:title"),
        "doi": e.get("prism:doi"),
        "pii": e.get("pii"),
        "authors": authors,
        "container_title": e.get("prism:publicationName"),
        "published": e.get("prism:coverDate"),
        "openaccess": e.get("openaccess"),
        "url": links.get("scidir") or links.get("self"),
        "abstract": None,
    }
