import json
d = json.load(open("data/collection.enriched.json", encoding="utf-8"))
missing = [f"{(x.get('artist_clean') or x.get('artist'))} — {x.get('title')}" for x in d if not x.get('release_date')]
print("\n".join(missing) if missing else "Todo con fecha ✅")