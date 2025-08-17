import json
d = json.load(open("data/collection.enriched.json", encoding="utf-8"))
bad = [x for x in d if x.get("release_date") and len(x["release_date"]) < 4]
print(f"Registros sospechosos: {len(bad)}")
for b in bad[:10]:
    print(b["artist_clean"], "—", b["title"], "=>", b["release_date"], b.get("release_source"))