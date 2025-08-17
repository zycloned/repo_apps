import os, json, datetime
import re

def _is_full_date(date_iso: str) -> bool:
    return isinstance(date_iso, str) and len(date_iso.split("-")) == 3

def ensure_data_dir():
    os.makedirs("data", exist_ok=True)

def save_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _next_anniv(date_iso: str, today=None):
    norm = _normalize_iso(date_iso)
    if not norm:
        return None, None

    y, m, d = [int(x) for x in norm.split("-")]
    today = today or datetime.date.today()
    try:
        this_year = datetime.date(today.year, m, d)
    except ValueError:
        if m == 2 and d == 29:
            this_year = datetime.date(today.year, 2, 28)
        else:
            return None, None

    if this_year < today:
        try:
            next_year = datetime.date(today.year + 1, m, d)
        except ValueError:
            if m == 2 and d == 29:
                next_year = datetime.date(today.year + 1, 2, 28)
            else:
                return None, None
        return next_year, (next_year - today).days
    else:
        return this_year, (this_year - today).days


def upcoming_anniversaries(data, days_ahead=7, include_partial=False):
    today = datetime.date.today()
    rows = []
    for it in data:
        rd = it.get("release_date")
        if not rd:
            continue
        if not include_partial and not _is_full_date(rd):
            continue  # omite YYYY o YYYY-MM
        next_dt, delta = _next_anniv(rd, today)
        if next_dt is None:
            continue
        if 0 <= delta <= days_ahead:
            rows.append({
                "artist_clean": it.get("artist_clean") or it.get("artist"),
                "title": it.get("title"),
                "release_date": rd,
                "release_source": it.get("release_source"),
                "release_url": it.get("release_url"),
                "next_anniv_date": next_dt.isoformat(),
                "days_left": delta
            })
    rows.sort(key=lambda r: (r["next_anniv_date"], r["artist_clean"], r["title"]))
    return rows

def _normalize_iso(date_iso: str):
    """
    Acepta 'YYYY', 'YYYY-MM' o 'YYYY-MM-DD' y devuelve siempre 'YYYY-MM-DD'.
    Ajusta 29-feb a 28-feb en años no bisiestos. Devuelve None si no es válida.
    """
    if not date_iso or not isinstance(date_iso, str):
        return None
    s = date_iso.strip()
    if not s or s.lower() in {"unknown", "n/a", "none", "null"}:
        return None

    parts = s.split("-")
    if len(parts) == 3:
        y, m, d = parts
    elif len(parts) == 2:
        y, m = parts
        d = "01"
    elif len(parts) == 1:
        y = parts[0]
        m, d = "01", "01"
    else:
        return None

    # Limpia cualquier sufijo raro en día/mes
    try:
        y = int(re.sub(r"\D", "", y))
        m = int(re.sub(r"\D", "", m))
        d = int(re.sub(r"\D", "", d))
        if m < 1 or m > 12:
            return None
        if d < 1 or d > 31:
            d = 1
    except Exception:
        return None

    try:
        return datetime.date(y, m, d).isoformat()
    except ValueError:
        # Maneja 29-feb en no bisiestos
        if m == 2 and d == 29:
            return datetime.date(y, 2, 28).isoformat()
        return None