import requests
from typing import Any, Dict

def generate_facebook_text(text_engine_url: str, slug: str, event: str, vehicle: Dict[str, Any]) -> str:
    base = (text_engine_url or "").rstrip("/")
    if not base:
        raise RuntimeError("KENBOT_TEXT_ENGINE_URL manquant.")
    url = f"{base}/generate"

    payload = {"slug": slug, "event": event, "vehicle": vehicle}
    r = requests.post(url, json=payload, timeout=60)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}

    if not r.ok:
        raise RuntimeError(f"Text engine error ({r.status_code}): {data}")

    txt = (data.get("facebook_text") or "").strip()
    if not txt:
        raise RuntimeError("Text engine a retourné un facebook_text vide.")
    return txt + "\n"
