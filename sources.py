import re, json, requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from unidecode import unidecode
import dateparser
import requests
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Crear carpeta data/ si no existe (para el archivo de caché)
Path("data").mkdir(parents=True, exist_ok=True)

try:
    import requests_cache  # pip install requests-cache
    S = requests_cache.CachedSession(
        "data/http_cache",      # archivo sqlite en data/
        backend="sqlite",
        expire_after=86400,     # 24 h
        allowable_methods=("GET",),
        stale_if_error=True,
    )
except Exception as e:
    # Si no está instalado o falla la caché, seguimos sin caché
    # print(f"[cache deshabilitada] {e}")  # <- opcional para depurar
    S = requests.Session()

S.headers.update({"User-Agent": "discogs-anniv-bot/1.1"})
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
S.mount("https://", HTTPAdapter(max_retries=retry))
S.mount("http://", HTTPAdapter(max_retries=retry))

DEFAULT_TIMEOUT = 20
def GET(url, **kwargs):
    timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
    return S.get(url, timeout=timeout, **kwargs)



UA = {"User-Agent": "discogs-anniv-bot/1.0"}


def _canon(s: str) -> str:
    s = unidecode((s or "").lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())

def _fuzzy(a: str, b: str) -> int:
    A, B = _canon(a), _canon(b)
    return max(fuzz.token_set_ratio(A, B), fuzz.partial_ratio(A, B))

def _ok(a, b, threshold=80):
    return _fuzzy(a, b) >= threshold

def _fuzzy_score(a: str, b: str) -> int:
    A, B = _canon(a), _canon(b)
    # mezcla de métricas para ser tolerantes con apóstrofes/guiones
    return max(
        fuzz.token_set_ratio(A, B),
        fuzz.partial_ratio(A, B)
    )

def _ok(a, b, threshold=80):
    return _fuzzy_score(a, b) >= threshold


def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _parse_date(text: str):
    if not text:
        return None
    dt = dateparser.parse(
        text,
        settings={"PREFER_DAY_OF_MONTH": "first", "RETURN_AS_TIMEZONE_AWARE": False}
    )
    return dt.date().isoformat() if dt else None


# ---------- Wikipedia ----------
def wikipedia_release_date(artist, title):
    # 1) buscar página candidata (igual que antes)
    q = f'{title} (album)'
    r = requests.get(
        "https://en.wikipedia.org/w/api.php",
        params={"action":"query","list":"search","format":"json","srsearch":q, "srlimit":5},
        headers=UA, timeout=30
    )
    r.raise_for_status()
    hits = r.json().get("query", {}).get("search", [])
    best = None
    for h in hits:
        if _ok(h.get("title",""), f"{title}"):
            best = h["title"]; break
    if not best and hits:
        best = hits[0]["title"]

    if not best:
        # fallback: 'artist title album'
        q = f'{artist} {title} album'
        r = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={"action":"query","list":"search","format":"json","srsearch":q, "srlimit":5},
            headers=UA, timeout=30
        )
        r.raise_for_status()
        hits = r.json().get("query", {}).get("search", [])
        for h in hits:
            if _ok(h.get("title",""), f"{title}"):
                best = h["title"]; break
        if not best and hits:
            best = hits[0]["title"]

    if not best:
        return None

    # 2) leer página
    r = requests.get(f"https://en.wikipedia.org/wiki/{best.replace(' ', '_')}", headers=UA, timeout=30)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "lxml")

    # ✅ validar que realmente es la página del álbum correcto
    h1 = soup.select_one("#firstHeading")
    page_title = h1.get_text(strip=True) if h1 else best
    if not _ok(page_title, title):
        return None  # no arriesgarse a tomar fechas de otra cosa

    # 3) tomar fecha SOLO de la infobox ("Released"/"Release date")
    infobox = soup.select_one(".infobox")
    if not infobox:
        return None

    for lab in infobox.select("tr th"):
        if lab.get_text(strip=True).lower() in {"released", "release date"}:
            val = lab.find_next("td")
            if not val:
                continue
            for sup in val.select("sup"):
                sup.decompose()
            txt = val.get_text(" ", strip=True).split(";")[0]
            return _parse_date(txt)

    return None  # sin fallback de escaneo global


# ---------- MusicBrainz ----------
def musicbrainz_release_date(artist, title):
    r = requests.get("https://musicbrainz.org/ws/2/release/", params={
        "query": f'release:"{title}" AND artist:"{artist}"',
        "fmt": "json", "limit": 10
    }, headers=UA, timeout=30)
    if r.status_code != 200:
        return None
    data = r.json()
    full, partial = [], []
    for rel in data.get("releases", []):
        t = rel.get("title","")
        arts = rel.get("artist-credit", [])
        aname = " ".join(ac.get("name","") for ac in arts)
        if _ok(t, title) and (_ok(aname, artist) or _ok(artist, aname)):
            d = rel.get("date")
            if d:
                (full if _is_full_date(d) else partial).append(d)
    if full:
        return sorted(full)[0], "musicbrainz", "https://musicbrainz.org"
    if partial:
        return sorted(partial)[0], "musicbrainz", "https://musicbrainz.org"
    return None


# ---------- Metal Archives (best-effort scraping) ----------
def metal_archives_release_date(artist, title, artist_clean=None):
    """
    Busca la banda en Metal Archives (intentando primero el nombre original y luego el 'clean'),
    abre la discografía completa y selecciona el álbum por fuzzy match.
    Devuelve (YYYY-MM-DD, 'metal-archives', url_del_album) o None.
    """
    # probamos ambos nombres para desambiguar homónimos (p.ej. "Odium (Nor)")
    for band_name in (artist, artist_clean or artist):
        # 1) Buscar banda
        r = GET(
            "https://www.metal-archives.com/search/ajax-advanced/searching/bands/",
            params={"bandName": band_name, "exactBandMatch": 0, "page": 1},
            headers=UA,
        )
        if r.status_code != 200:
            continue
        js = r.json()
        aa = js.get("aaData") or []
        if not aa:
            continue

        # elegir la mejor banda por fuzzy score
        best_band_url = None
        best_band_score = -1
        best_band_name = None
        for row in aa:
            # row[0] es HTML con <a href="...">Nombre</a>
            m = re.search(r'href="([^"]+)"[^>]*>(.*?)</a>', row[0])
            if not m:
                continue
            url = m.group(1)
            disp = BeautifulSoup(m.group(2), "lxml").get_text()
            score = _fuzzy_score(disp, band_name)
            if score > best_band_score:
                best_band_score = score
                best_band_url = url
                best_band_name = disp

        # umbral razonable; si no alcanza, probamos con el siguiente band_name
        if not best_band_url or best_band_score < 70:
            continue

        # 2) Discografía completa
        # best_band_url suele ser /bands/<Name>/<id>
        disc_url = best_band_url.replace("/bands/", "/band/discography/id/") + "/tab/all"
        r = GET(disc_url, headers=UA)
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select("table.display tbody tr")
        if not rows:
            continue

        # elegir mejor álbum por fuzzy score
        best_link = None
        best_score = -1
        best_title = None
        for tr in rows:
            cols = tr.find_all("td")
            if len(cols) >= 1:
                a = cols[0].find("a")
                if not a:
                    continue
                album_title = a.get_text(strip=True)
                score = _fuzzy_score(album_title, title)
                if score > best_score:
                    best_score = score
                    best_link = a.get("href")
                    best_title = album_title

        # si no pasó el umbral fuerte (80), acepta 75 como fallback
        if not best_link or best_score < 75:
            continue

        # 3) Página del álbum -> "Release date:"
        r = GET(best_link, headers=UA)
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "lxml")

        date_text = None
        for row in soup.select("#album_info dt"):
            if row.get_text(strip=True).lower().startswith("release date"):
                dd = row.find_next_sibling("dd")
                if dd:
                    txt = dd.get_text(" ", strip=True)
                    # limpiar ordinales tipo "August 18th, 2016"
                    txt = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", txt)
                    date_text = txt
                break

        if date_text:
            d = _parse_date(date_text)
            if d:
                return d, "metal-archives", best_link

    return None


# helpers que ya deberías tener:
def _is_full_date(date_str: str) -> bool:
    return isinstance(date_str, str) and len(date_str.split("-")) == 3

def find_release_date(artist_clean, title, artist_original=None):
    """
    Intenta con fuentes rápidas primero. Si una fuente devuelve fecha completa (YYYY-MM-DD),
    corta. Para Metal Archives se prueban ambos nombres: original y 'clean'.
    """
    name_primary = (artist_original or artist_clean or "").strip()
    best_partial = None

    # 1) Rápidas
    for fn in (musicbrainz_label_event_date, musicbrainz_release_date, wikipedia_release_date):
        try:
            r = fn(name_primary, title)
        except Exception:
            r = None
        if not r:
            continue

        if isinstance(r, tuple):
            date, source, url = (r + (None, None))[:3]
        else:
            date, source, url = r, fn.__name__, None

        if isinstance(date, str) and date:
            if _is_full_date(date):
                return {"date": date, "source": source, "url": url}
            if best_partial is None:
                best_partial = {"date": date, "source": source, "url": url}

    # 2) Metal Archives (usa original + clean)
    try:
        r = metal_archives_release_date(artist=name_primary, title=title, artist_clean=artist_clean)
    except Exception:
        r = None
    if r:
        date, source, url = (r + (None, None))[:3]
        if isinstance(date, str) and date:
            if _is_full_date(date):
                return {"date": date, "source": source, "url": url}
            if best_partial is None:
                best_partial = {"date": date, "source": source, "url": url}

    # 3) Bandcamp via MusicBrainz (más lento)
    try:
        r = bandcamp_release_date_via_musicbrainz(name_primary, title)
    except Exception:
        r = None
    if r:
        date, source, url = (r + (None, None))[:3]
        if isinstance(date, str) and date:
            if _is_full_date(date):
                return {"date": date, "source": source, "url": url}
            if best_partial is None:
                best_partial = {"date": date, "source": source, "url": url}

    return best_partial




# ---------- BANDCAMP (vía MusicBrainz url-rels) ----------
def _bandcamp_extract_date(page_html: str):
    # Intenta leer fecha desde el bloque JS TralbumData o data-blob
    # 1) JSON embebido en window.TralbumData
    m = re.search(r"TralbumData\s*=\s*({.*?});\s*\n", page_html, re.S)
    if m:
        blob = m.group(1)
        # Está en JS, intenta localizar campos de fecha sin convertir todo a JSON
        for key in ("album_release_date", "release_date", "publish_date", "publish_date_utc"):
            mm = re.search(rf'"{key}"\s*:\s*"([^"]+)"', blob)
            if mm:
                d = _parse_date(mm.group(1))
                if d:
                    return d
    # 2) meta tags sueltos (fallback muy general)
    soup = BeautifulSoup(page_html, "lxml")
    # A veces aparece en <meta itemprop="datePublished" content="2016-08-19">
    tag = soup.find("meta", attrs={"itemprop": "datePublished"}) or \
          soup.find("meta", attrs={"property": "music:release_date"}) or \
          soup.find("meta", attrs={"property": "og:release_date"})
    if tag and tag.get("content"):
        d = _parse_date(tag["content"])
        if d:
            return d
    # 3) escaneo leve del cuerpo
    mm = re.search(r"\b([A-Z][a-z]+ \d{1,2}, \d{4}|\d{4}-\d{2}-\d{2})\b", soup.get_text(" ", strip=True))
    if mm:
        return _parse_date(mm.group(1))
    return None

def bandcamp_release_date_via_musicbrainz(artist, title):
    """
    Busca el release group en MusicBrainz y lee relaciones de URL.
    Si hay un link a bandcamp.com/album/..., descarga esa página y extrae la fecha.
    Preferentemente corresponde al sello si el link es del sello; si no, igual sirve.
    """
    # 1) Buscar release-group por artista + título
    r = requests.get("https://musicbrainz.org/ws/2/release-group", params={
        "query": f'releasegroup:"{title}" AND artist:"{artist}"',
        "fmt": "json", "limit": 5
    }, headers=UA, timeout=30)
    if r.status_code != 200:
        return None
    data = r.json()
    best = None
    best_score = -1
    for rg in data.get("release-groups", []):
        t = rg.get("title", "")
        score = _fuzzy(t, title)
        if score > best_score:
            best_score = score
            best = rg
    if not best or best_score < 75:
        return None

    mbid = best.get("id")
    # 2) Traer relaciones de URL para hallar bandcamp
    r = requests.get(f"https://musicbrainz.org/ws/2/release-group/{mbid}", params={
        "fmt": "json", "inc": "url-rels"
    }, headers=UA, timeout=30)
    if r.status_code != 200:
        return None
    rels = r.json().get("relations", []) or []
    # Tomar primero un album-link en bandcamp
    bc_links = [rel.get("url", {}).get("resource", "") for rel in rels if "bandcamp.com/album" in rel.get("url", {}).get("resource", "")]
    if not bc_links:
        # si no hay /album, toma cualquier bandcamp
        bc_links = [rel.get("url", {}).get("resource", "") for rel in rels if "bandcamp.com" in rel.get("url", {}).get("resource", "")]
    for url in bc_links:
        try:
            p = requests.get(url, headers=UA, timeout=30)
            if p.status_code == 200:
                d = _bandcamp_extract_date(p.text)
                if d:
                    return d, "bandcamp", url
        except Exception:
            continue
    return None

# ---------- MUSICBRAINZ (release events: fecha por edición/label) ----------
def musicbrainz_label_event_date(artist, title):
    r = requests.get("https://musicbrainz.org/ws/2/release", params={
        "query": f'release:"{title}" AND artist:"{artist}"',
        "fmt": "json", "limit": 15, "inc": "labels+release-groups+release-events"
    }, headers=UA, timeout=30)
    if r.status_code != 200:
        return None
    data = r.json()
    full, partial = [], []
    for rel in data.get("releases", []):
        t = rel.get("title", "")
        arts = " ".join(ac.get("name", "") for ac in rel.get("artist-credit", []))
        if not (_ok(t, title) and (_ok(arts, artist) or _ok(artist, arts))):
            continue
        # fecha directa
        d = rel.get("date")
        if d:
            (full if _is_full_date(d) else partial).append(d)
        # por eventos
        for ev in rel.get("release-events", []) or []:
            d2 = ev.get("date")
            if d2:
                (full if _is_full_date(d2) else partial).append(d2)
    if full:
        d = sorted(full)[0]
        return d, "musicbrainz", "https://musicbrainz.org"
    if partial:
        d = sorted(partial)[0]
        return d, "musicbrainz", "https://musicbrainz.org"
    return None
