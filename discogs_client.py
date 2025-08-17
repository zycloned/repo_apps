import os, math, time, re, json
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from utils import ensure_data_dir, save_json

load_dotenv()

BASE = "https://api.discogs.com"
USERNAME = os.getenv("DISCOGS_USERNAME", "").strip()
TOKEN = os.getenv("DISCOGS_TOKEN", "").strip()

HEADERS = {
    "User-Agent": "discogs-anniv-bot/1.0",
    "Authorization": f"Discogs token={TOKEN}",
}

def _clean_artist_name(name: str) -> str:
    # Solo elimina sufijos (número) al final: "Emperor (2)" -> "Emperor"
    # Conserva "(Nor)", "(Swe)", etc., que ayudan a desambiguar bandas homónimas
    return re.sub(r"\s*\(\d+\)\s*$", "", name or "").strip()

def fetch_collection(per_page=100):
    if not USERNAME or not TOKEN:
        raise RuntimeError("Configura DISCOGS_USERNAME y DISCOGS_TOKEN en .env")

    ensure_data_dir()

    url = f"{BASE}/users/{USERNAME}/collection/folders/0/releases"
    params = {"per_page": per_page, "page": 1}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    first = resp.json()
    total = first.get("pagination", {}).get("items", 0)
    pages = first.get("pagination", {}).get("pages", 1)

    items = []
    def extract(page_json):
        for it in page_json.get("releases", []):
            basic = it.get("basic_information", {})
            title = (basic.get("title") or "").strip()
            artists = basic.get("artists", []) or []
            artist = (artists[0].get("name") if artists else "").strip()
            formats = [f.get("name") for f in (basic.get("formats") or []) if f.get("name")]
            item = {
                "artist": artist,
                "artist_clean": _clean_artist_name(artist),
                "title": title,
                "formats": formats,
            }
            items.append(item)

        labels = []
        for lab in (basic.get("labels") or []):
            nm = lab.get("name")
            if nm:
                labels.append(nm)

        item = {
            "artist": artist,
            "artist_clean": _clean_artist_name(artist),
            "title": title,
            "formats": formats,
            "labels": labels,  # ← nuevo
        }    

    extract(first)
    if pages > 1:
        for page in tqdm(range(2, pages + 1), desc="Descargando colección"):
            params["page"] = page
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            extract(r.json())
            time.sleep(0.2)  # cuida rate limit

    # Guardar
    save_json(items, "data/collection.raw.json")
    return len(items)
