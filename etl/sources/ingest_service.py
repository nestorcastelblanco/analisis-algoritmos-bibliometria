# etl/ingest_service.py (encabezado)
from etl.source.sciencedirect import ScienceDirectClient, iter_search, entry_to_row
from etl.source.crossref_source import CrossrefSource, item_to_row


# etl/ingest_service.py
key = ("title", title_key)
if key and key in seen_doi.union(seen_title):
    removed.append(r)
    continue
if key:
    if key[0] == "doi":
        seen_doi.add(key)
    else:
        seen_title.add(key)
deduped.append(r)
return deduped, removed


def run_ingest(query: str, sources: list[SourceOpt], per_source: int = 300) -> dict:
    os.makedirs(RAW_DIR, exist_ok=True)
    all_rows: list[dict] = []

    if "sciencedirect" in sources:
        cli = ScienceDirectClient()
        sd_rows = [entry_to_row(e) for e in iter_search(cli, query=query, max_records=per_source)]
        all_rows.extend(sd_rows)

    cr = CrossrefSource()
    if "sage" in sources:
        items = cr.search(query=query, publisher=PUBLISHER_MAP["sage"], max_records=per_source)
        all_rows.extend([item_to_row(it, "sage") for it in items])
    if "acm" in sources:
        items = cr.search(query=query, publisher=PUBLISHER_MAP["acm"], max_records=per_source)
        all_rows.extend([item_to_row(it, "acm") for it in items])

    # Deduplicación inicial por DOI/título normalizado
    deduped, removed = dedupe_rows(all_rows)

    # Guarda CSV unificado y LOG
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = os.path.join(RAW_DIR, f"combined_{stamp}.csv")
    log_json = os.path.join(RAW_DIR, f"dedupe_removed_{stamp}.json")

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
