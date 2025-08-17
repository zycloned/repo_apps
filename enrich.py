from utils import load_json, save_json
from sources import find_release_date
from tqdm import tqdm   # <--- agrega esta importación
import json, os
from utils import load_json, save_json
from concurrent.futures import ThreadPoolExecutor, as_completed

def _load_overrides():
    p = "data/overrides.json"
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return {(o["artist"].lower(), o["title"].lower()): o["release_date"] for o in json.load(f)}
    return {}

def enrich_release_dates(max_workers=6, only_missing=False):
    data = load_json("data/collection.raw.json") or []
    # dedup por artista+título
    seen, items = set(), []
    for it in data:
        key = ((it.get("artist_clean") or it.get("artist") or "").strip().lower(),
               (it.get("title") or "").strip().lower())
        if key in seen: 
            continue
        seen.add(key)
        items.append(it)

    # si hay enriched previo y only_missing=True, carga y salta los que ya tienen fecha
    existing = {}
    if only_missing:
        prev = load_json("data/collection.enriched.json") or []
        for row in prev:
            k = ((row.get("artist_clean") or row.get("artist") or "").strip().lower(),
                 (row.get("title") or "").strip().lower())
            existing[k] = row

    def worker(it):
        artist = (it.get("artist") or it.get("artist_clean") or "").strip()
        artist_orig  = (it.get("artist") or "").strip()
        artist_clean = (it.get("artist_clean") or artist_orig).strip()
        title        = (it.get("title") or "").strip()

        if only_missing:
            k = (artist.lower(), title.lower())
            old = existing.get(k)
            if old and old.get("release_date"):
                # ya lo teníamos, devolver tal cual
                return {**it, **{k: old.get(k) for k in ("release_date","release_source","release_url")}}, bool(old.get("release_date"))
        info = find_release_date(artist_clean, title, artist_original=artist_orig)
        row = {**it, "release_date": None, "release_source": None, "release_url": None}
        if isinstance(info, dict) and info.get("date"):
            row["release_date"] = info["date"]
            row["release_source"] = info.get("source")
            row["release_url"]    = info.get("url")
            return row, True
        return row, False

    out, n_ok = [], 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(worker, it) for it in items]
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Buscando fechas", unit="rel"):
            row, ok = fut.result()
            if ok: n_ok += 1
            out.append(row)

    save_json(out, "data/collection.enriched.json")
    return n_ok, len(items)


def enrich_missing_only():
    data = load_json("data/collection.enriched.json") or []
    if not data:
        print("No existe data/collection.enriched.json. Ejecuta primero: python app.py enrich")
        return 0, 0

    n_new = 0
    for it in tqdm([x for x in data if not x.get("release_date")], desc="Reintentando faltantes"):
        artist_orig  = (it.get("artist") or "").strip()
        artist_clean = (it.get("artist_clean") or artist_orig).strip()
        title        = (it.get("title") or "").strip()

        info = find_release_date(artist_clean, title, artist_original=artist_orig)
        if isinstance(info, dict) and info.get("date"):
            it["release_date"]  = info["date"]
            it["release_source"] = info.get("source")
            it["release_url"]    = info.get("url")
            n_new += 1

    save_json(data, "data/collection.enriched.json")
    return n_new, len(data)

