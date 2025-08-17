import argparse
from discogs_client import fetch_collection
from enrich import enrich_release_dates, enrich_missing_only
from utils import upcoming_anniversaries, load_json, ensure_data_dir

def cmd_update():
    ensure_data_dir()
    count = fetch_collection()
    print(f"OK. Se guardaron {count} ítems en data/collection.raw.json")

def cmd_enrich():
    ensure_data_dir()
    n_ok, n_total = enrich_release_dates()
    print(f"Fechas encontradas para {n_ok}/{n_total} lanzamientos. Archivo: data/collection.enriched.json")

def cmd_anniv():
    ensure_data_dir()
    print("[anniversaries] Leyendo data/collection.enriched.json …")
    data = load_json("data/collection.enriched.json")
    if not data:
        print("Primero ejecuta: python app.py enrich")
        return
    rows = upcoming_anniversaries(data, days_ahead=7, include_partial=False)
    if not rows:
        print("No hay aniversarios en los próximos 7 días.")
        return
    print("Aniversarios próximos (7 días):")
    for r in rows:
        src = f" · fuente: {r['release_source']}" if r.get("release_source") else ""
        print(f"- {r['artist_clean']} — {r['title']} | Lanzamiento: {r['release_date']} | Día: {r['next_anniv_date']}{src}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Discogs anniversaries")
    ap.add_argument("command", choices=["update", "enrich", "anniversaries", "month", "retry-missing", "all"], help="Qué quieres ejecutar")

    args = ap.parse_args()

    if args.command == "update":
        cmd_update()
    elif args.command == "enrich":
        cmd_enrich()
    elif args.command == "retry-missing":
        ensure_data_dir()
        print("[retry-missing] Reintentando sólo los que no tienen fecha…")
        n_new, total = enrich_missing_only()
        print(f"Nuevas fechas encontradas: {n_new}. Total items: {total}")        
    elif args.command == "anniversaries":
        cmd_anniv()
    elif args.command == "all":
        cmd_update()
        cmd_enrich()
        cmd_anniv()
