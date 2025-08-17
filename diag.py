# diag.py
from pathlib import Path
import os, requests, json
from dotenv import load_dotenv

print("== Diagnóstico ==")
print("CWD:", Path.cwd())

# Carga .env desde la carpeta actual y (por si acaso) desde donde está discogs_client.py
load_dotenv()  # cwd

# Muestra lo que ve Python
USER = os.getenv("DISCOGS_USERNAME")
TOKEN = os.getenv("DISCOGS_TOKEN")
print("DISCOGS_USERNAME:", USER)
print("DISCOGS_TOKEN definido?:", bool(TOKEN))

if not USER or not TOKEN:
    print("Faltan variables en .env (asegúrate de tener .env en la RAÍZ, sin extensión extra).")
    raise SystemExit(1)

# Prueba endpoint simple (cuenta de usuario)
hdrs = {
    "User-Agent": "discogs-anniv-bot/diag",
    "Authorization": f"Discogs token={TOKEN}",
}
url = f"https://api.discogs.com/users/{USER}"
r = requests.get(url, headers=hdrs, timeout=30)
print("GET", url, "->", r.status_code)

if r.status_code == 200:
    js = r.json()
    print("OK. Usuario válido. Ejemplo:", {k: js.get(k) for k in ("username", "id", "resource_url")})
else:
    print("Respuesta:", r.text[:300])
    print("Si es 401: revisa el TOKEN. Si es 404: revisa el USERNAME.")
