# etl/sources/sciencedirect.py
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


def search(self, query: str, count: int = 100, start: int = 0) -> dict:
params = {"query": query, "count": min(count, 100), "start": start}
url = f"{SD_SEARCH_URL}?{urlencode(params)}"
r = self.session.get(url, timeout=self.timeout)
r.raise_for_status()
return r.json()


def fetch_article_meta(self, pii: str, view: str = "META_ABS") -> dict:
url = SD_ARTICLE_PII_URL.format(pii)
r = self.session.get(url, params={"view": view}, timeout=self.timeout)
r.raise_for_status()
return r.json()




def iter_search(client: ScienceDirectClient, query: str, max_records: int = 300) -> t.Iterator[dict]:
got, start = 0, 0
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
time.sleep(0.35) # cortesía de rate limit




def entry_to_row(e: dict) -> dict:
# Normaliza campos a un esquema CSV común
links = {l.get("@ref"): l.get("@href") for l in (e.get("link") or [])}
return {
"source": "sciencedirect",
"title": e.get("dc:title"),
"doi": e.get("prism:doi"),
"pii": e.get("pii"),
"authors": "; ".join([a.get("authname", "") for a in (e.get("authors", {}).get("author") or [])]) if e.get("authors") else None,
"container_title": e.get("prism:publicationName"),
"published": e.get("prism:coverDate"),
"openaccess": e.get("openaccess"),
"url": links.get("scidir") or links.get("self"),
"abstract": None, # opcional: completar con fetch_article_meta si es necesario
}