import time
import requests

def generate_facebook_text(base_url: str, slug: str, event: str, vehicle: dict) -> str:
    url = f"{base_url.rstrip('/')}/generate"
    payload = {"slug": slug, "event": event, "vehicle": vehicle}

    last_err = None
    for attempt in range(1, 4):  # 3 essais
        try:
            r = requests.post(url, json=payload, timeout=90)
            r.raise_for_status()
            j = r.json()
            return (j.get("text") or "").strip()
        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)

    # fallback minimal: pas beau, mais ça publie
    v = vehicle or {}
    return (
        f"🔥 {v.get('title','Véhicule')} 🔥\n\n"
        f"💰 {v.get('price','')}\n"
        f"📊 {v.get('mileage','')}\n"
        f"🧾 Stock : {v.get('stock','')}\n"
        f"🔢 VIN : {v.get('vin','')}\n\n"
        f"{v.get('url','')}\n"
        f"\n⚠️ (Texte généré en mode secours — service AI indisponible)"
    )
