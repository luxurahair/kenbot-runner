import os
import re
import hashlib
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

from kennebec_scrape import (
    parse_inventory_listing_urls,
    parse_vehicle_detail_simple,
    slugify,
)
from text_engine_client import generate_facebook_text
from fb_api import (
    publish_photos_unpublished,
    create_post_with_attached_media,
    update_post_text,
    publish_photos_as_comment_batch,
)
from supabase_db import (
    get_client,
    get_inventory_map,
    get_posts_map,
    upsert_inventory,
    upsert_post,
    log_event,
    utc_now_iso,
)

# Load env (local dev only; on Render, env vars are injected)
for name in (".env.local", ".kenbot_env", ".env"):
    p = Path(name)
    if p.exists():
        load_dotenv(p, override=False)
        break

BASE_URL = os.getenv("KENBOT_BASE_URL", "https://www.kennebecdodge.ca").rstrip("/")
INVENTORY_PATH = os.getenv("KENBOT_INVENTORY_PATH", "/fr/inventaire-occasion/")
TEXT_ENGINE_URL = os.getenv("KENBOT_TEXT_ENGINE_URL", "").strip()

FB_PAGE_ID = (os.getenv("KENBOT_FB_PAGE_ID") or os.getenv("FB_PAGE_ID") or "").strip()
FB_TOKEN   = (os.getenv("KENBOT_FB_ACCESS_TOKEN") or os.getenv("FB_PAGE_ACCESS_TOKEN") or "").strip()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

STICKERS_BUCKET = os.getenv("SB_BUCKET_STICKERS", "kennebec-stickers").strip()
RAW_BUCKET = os.getenv("SB_BUCKET_RAW", "kennebec-raw").strip()
OUTPUTS_BUCKET = os.getenv("SB_BUCKET_OUTPUTS", "kennebec-outputs").strip()

DRY_RUN = os.getenv("KENBOT_DRY_RUN", "0").strip() == "1"
FORCE_STOCK = (os.getenv("KENBOT_FORCE_STOCK") or "").strip().upper()
MAX_TARGETS = int(os.getenv("KENBOT_MAX_TARGETS", "4").strip() or "4")
SLEEP_BETWEEN = int(os.getenv("KENBOT_SLEEP_BETWEEN_POSTS", "60").strip() or "60")

if not FB_PAGE_ID or not FB_TOKEN:
    raise SystemExit("🛑 FB creds manquants: KENBOT_FB_PAGE_ID + KENBOT_FB_ACCESS_TOKEN")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("🛑 Supabase creds manquants: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY")
if not TEXT_ENGINE_URL:
    raise SystemExit("🛑 KENBOT_TEXT_ENGINE_URL manquant (kenbot-text-engine)")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15",
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
})

def _sha256(b: bytes) -> str:
    return hashlib.sha256(b or b"").hexdigest()

def _is_pdf_ok(b: bytes) -> bool:
    return bool(b) and len(b) >= 10_240 and b[:4] == b"%PDF"

def _is_stellantis_vin(vin: str) -> bool:
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return False
    # Stellantis fréquents chez toi: 1C/2C/3C + Jeep Italy ZAC + Fiat ZFA
    return (
        vin.startswith(("1C", "2C", "3C")) or
        vin.startswith(("ZAC", "ZFA"))
    )

def _storage_download_or_none(sb, bucket: str, path: str) -> bytes | None:
    try:
        return sb.storage.from_(bucket).download(path)
    except Exception:
        return None

def ensure_sticker_cached(sb, vin: str, run_id: str = "") -> dict:
    """
    Assure qu'on a un sticker en cache Storage:
    - pdf_ok/<VIN>.pdf si valide
    - pdf_bad/<VIN>.pdf si invalide (<10KB ou pas %PDF)
    Upsert sticker_pdfs (DB).
    """
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return {"vin": vin, "status": "skip", "reason": "vin_invalid"}

    ok_path = f"pdf_ok/{vin}.pdf"
    bad_path = f"pdf_bad/{vin}.pdf"

    # 1) Déjà en cache OK ?
    data = _storage_download_or_none(sb, STICKERS_BUCKET, ok_path)
    if _is_pdf_ok(data or b""):
        meta = {"vin": vin, "status": "ok", "storage_path": ok_path, "bytes": len(data), "sha256": _sha256(data), "reason": None, "run_id": run_id}
        sb.table("sticker_pdfs").upsert(meta).execute()
        return meta

    # 2) Déjà en cache BAD ?
    data_bad = _storage_download_or_none(sb, STICKERS_BUCKET, bad_path)
    if data_bad is not None and (len(data_bad) > 0):
        meta = {"vin": vin, "status": "bad", "storage_path": bad_path, "bytes": len(data_bad), "sha256": _sha256(data_bad), "reason": "cached_bad", "run_id": run_id}
        sb.table("sticker_pdfs").upsert(meta).execute()
        return meta

    # 3) Télécharger Chrysler (runner = seul fetcher)
    pdf_url = f"https://www.chrysler.com/hostd/windowsticker/getWindowStickerPdf.do?vin={vin}"
    try:
        r = SESSION.get(pdf_url, timeout=25)
        blob = r.content or b""
    except Exception:
        blob = b""

    if _is_pdf_ok(blob):
        sb.storage.from_(STICKERS_BUCKET).upload(
            ok_path,
            blob,
            {"content-type": "application/pdf", "upsert": "true"},
        )
        meta = {"vin": vin, "status": "ok", "storage_path": ok_path, "bytes": len(blob), "sha256": _sha256(blob), "reason": None, "run_id": run_id}
        sb.table("sticker_pdfs").upsert(meta).execute()
        return meta

    # BAD -> upload pour audit (optionnel, mais utile)
    sb.storage.from_(STICKERS_BUCKET).upload(
        bad_path,
        blob or b"x",
        {"content-type": "application/pdf", "upsert": "true"},
    )
    meta = {"vin": vin, "status": "bad", "storage_path": bad_path, "bytes": len(blob or b""), "sha256": _sha256(blob or b""), "reason": "invalid_pdf", "run_id": run_id}
    sb.table("sticker_pdfs").upsert(meta).execute()
    return meta

TMP_PHOTOS = Path(os.getenv("KENBOT_TMP_PHOTOS_DIR", "/tmp/kenbot_photos"))
TMP_PHOTOS.mkdir(parents=True, exist_ok=True)

MAX_PHOTOS = 15
POST_PHOTOS = 10  # 10 dans le post, 5 extra best-effort

def download_photos(stock: str, urls: List[str], limit: int) -> List[Path]:
    out: List[Path] = []
    stock = (stock or "UNKNOWN").strip().upper()
    folder = TMP_PHOTOS / stock
    folder.mkdir(parents=True, exist_ok=True)

    for i, u in enumerate(urls[:limit], start=1):
        ext = ".jpg"
        low = (u or "").lower()
        if ".png" in low:
            ext = ".png"
        elif ".webp" in low:
            ext = ".webp"

        p = folder / f"{stock}_{i:02d}{ext}"
        if not p.exists():
            try:
                download_photo(u, p)
            except Exception:
                continue
        out.append(p)
    return out

def download_photos(stock: str, urls: List[str], limit: int) -> List[Path]:
    out: List[Path] = []
    stock = (stock or "UNKNOWN").strip().upper()
    folder = TMP_PHOTOS / stock
    folder.mkdir(parents=True, exist_ok=True)

    for i, u in enumerate(urls[:limit], start=1):
        ext = ".jpg"
        low = (u or "").lower()
        if ".png" in low:
            ext = ".png"
        elif ".webp" in low:
            ext = ".webp"
        p = folder / f"{stock}_{i:02d}{ext}"
        if not p.exists():
            try:
                download_photo(u, p)
            except Exception:
                continue
        out.append(p)
    return out

def sold_prefix() -> str:
    return (
        "🚨 VENDU 🚨\n\n"
        "Ce véhicule n’est plus disponible.\n\n"
        "👉 Vous recherchez un véhicule semblable ?\n"
        "Contactez-moi directement, je peux vous aider à en trouver un rapidement.\n\n"
        "Daniel Giroux\n"
        "📞 418-222-3939\n"
        "────────────────────\n\n"
    )

def _preview_text(slug: str, event: str, fb_text: str) -> None:
    preview = (fb_text or "").strip()
    if len(preview) > 900:
        preview = preview[:900] + "\n... [TRUNCATED]"
    print(f"\n========== DRY_RUN {event}: {slug} ==========\n{preview}\n==============================================\n")

def _clean_km(x):
    if x is None:
        return None
    try:
        s = str(x).replace(" ", "").replace("\u00a0", "").replace(",", "")
        km = int(s)
    except Exception:
        return None
    if km <= 0 or km > 500_000:
        return None
    return km

def _clean_price_int(x):
    if x is None:
        return None
    try:
        s = str(x).replace(" ", "").replace("\u00a0", "").replace(",", "").replace("$", "")
        p = int(s)
    except Exception:
        return None
    if p <= 0 or p > 500_000:
        return None
    return p

def _clean_title(t: str) -> str:
    t = (t or "").strip()
    low = t.lower()
    # titres trop génériques = scrap incomplet
    if low in {"jeep", "dodge", "ram", "chrysler", "fiat", "hyundai", "mazda", "mercedes", "polaris"}:
        return ""
    if len(t) < 6:
        return ""
    return t

def _run_id_from_now(now_iso: str) -> str:
    """
    Génère un run_id stable et lisible à partir d'un timestamp ISO (utc_now_iso()).
    Exemple: 20260118_212530
    """
    s = (now_iso or "").strip()
    # garde seulement chiffres pour être safe (Supabase/storage friendly)
    digits = "".join(ch for ch in s if ch.isdigit())
    # ISO typique: YYYYMMDDHHMMSS...
    if len(digits) >= 14:
        return f"{digits[0:8]}_{digits[8:14]}"
    return f"run_{int(time.time())}"

def dealer_footer() -> str:
    return (
        "\n\n🔁 J’accepte les échanges : 🚗 auto • 🏍️ moto • 🛥️ bateau • 🛻 VTT • 🏁 côte-à-côte\n"
        "📸 Envoie-moi les photos + infos de ton échange (année / km / paiement restant) → je te reviens vite.\n\n"
        "👋 Publiée par Daniel Giroux — je réponds vite (pas un robot, promis 😄)\n"
        "📍 Saint-Georges (Beauce) | Prise de possession rapide possible\n"
        "📄 Vente commerciale — 2 taxes applicables\n"
        "✅ Inspection complète — véhicule propre & prêt à partir.\n\n"
        "📩 Écris-moi en privé — ou texte direct\n"
        "📞 Daniel Giroux — 418-222-3939\n\n"
        "#RAM #Truck #Pickup #ProMaster #Cargo #Van #RAM1500 #Beauce #SaintGeorges #Quebec "
        "#AutoUsagée #VehiculeOccasion #DanielGiroux"
    )

def rebuild_posts_map(page_id: str, access_token: str, limit: int = 300) -> dict:
    posts_map = {}
    fetched = 0
    after = None

    while fetched < limit:
        params = {
            "fields": "id,message,created_time,permalink_url",
            "limit": 25,
            "access_token": access_token,
        }
        if after:
            params["after"] = after

        url = f"https://graph.facebook.com/v24.0/{page_id}/posts"
        r = SESSION.get(url, params=params, timeout=60)
        j = r.json()
        if not r.ok:
            raise RuntimeError(f"FB posts fetch failed: {j}")

        data = j.get("data") or []
        if not data:
            break

        for item in data:
            fetched += 1
            msg = (item.get("message") or "").strip()
            post_id = item.get("id")
            created = item.get("created_time") or ""
            if not post_id:
                continue

            # Stock: 06193, 45211A, etc.
            m = re.search(r"\b(\d{5}[A-Za-z]?)\b", msg)
            stock = (m.group(1).upper() if m else "")
            if not stock:
                continue

            posts_map[stock] = {
                "post_id": post_id,
                "published_at": created,
            }

            if fetched >= limit:
                break

        paging = (j.get("paging") or {}).get("cursors") or {}
        after = paging.get("after")
        if not after:
            break

    return posts_map

def main() -> None:
    sb = get_client(SUPABASE_URL, SUPABASE_KEY)

    inv_db = get_inventory_map(sb)
    posts_db = get_posts_map(sb)

    REBUILD = os.getenv("KENBOT_REBUILD_POSTS", "0").strip() == "1"

    if REBUILD:
        try:
            fb_map = rebuild_posts_map(FB_PAGE_ID, FB_TOKEN, limit=300)
            updated = 0

            # Match stock -> slug via inv_db
            for slug, inv in inv_db.items():
                stock = (inv.get("stock") or "").strip().upper()
                if not stock:
                    continue
                info = fb_map.get(stock)
                if not info:
                    continue

                upsert_post(sb, {
                    "slug": slug,
                    "post_id": info["post_id"],
                    "status": "ACTIVE",
                    "published_at": info.get("published_at"),
                    "last_updated_at": utc_now_iso(),
                })
                updated += 1

            log_event(sb, "REBUILD_POSTS", "REBUILD_POSTS_OK", {"fb_found": len(fb_map), "updated": updated})

            # refresh posts_db en mémoire pour le reste du run
            posts_db = get_posts_map(sb)

        except Exception as e:
            log_event(sb, "REBUILD_POSTS", "REBUILD_POSTS_FAIL", {"err": str(e)})

    # 1) Fetch listing pages (3 pages)
    now = utc_now_iso()

    urls: List[str] = []
    pages_html: List[Tuple[int, str]] = []
    pages = [
        f"{BASE_URL}{INVENTORY_PATH}",
        f"{BASE_URL}{INVENTORY_PATH}?page=2",
        f"{BASE_URL}{INVENTORY_PATH}?page=3",
    ]
    for idx, page in enumerate(pages, start=1):
        html = SESSION.get(page, timeout=30).text
        pages_html.append((idx, html))
        urls += parse_inventory_listing_urls(BASE_URL, INVENTORY_PATH, html)

    urls = sorted(list(dict.fromkeys(urls)))

    run_id = _run_id_from_now(now)

    meta = {
        "run_id": run_id,
        "base_url": BASE_URL,
        "inventory_path": INVENTORY_PATH,
        "pages": pages,
        "count_urls": len(urls),
        "dry_run": DRY_RUN,
        "force_stock": FORCE_STOCK,
    }

    try:
        upload_raw_pages(sb, run_id, pages_html, meta)
    except Exception as e:
        log_event(sb, "RAW_UPLOAD", "RAW_UPLOAD_FAIL", {"err": str(e), "run_id": run_id})
    
    # 2) Build current inventory map
    current: Dict[str, Dict[str, Any]] = {}
    for url in urls:
        d = parse_vehicle_detail_simple(SESSION, url)
        stock = (d.get("stock") or "").strip().upper()
        title = _clean_title((d.get("title") or "").strip())
        if not stock or not title:
            continue
        d["title"] = title
        slug = slugify(title, stock)
        d["slug"] = slug
        current[slug] = d

    if os.getenv("KENBOT_DEBUG_STOCKS", "0").strip() == "1":
        print("SAMPLES_STOCKS:", sorted({(v.get("stock") or "").strip().upper() for v in current.values()})[:30])

    current_slugs = set(current.keys())
    db_slugs = set(inv_db.keys())

    new_slugs = sorted(current_slugs - db_slugs)
    disappeared_slugs = sorted(db_slugs - current_slugs)
    common_slugs = sorted(current_slugs & db_slugs)

    now = utc_now_iso()

    # 3) Upsert inventory rows ACTIVE
    rows = []
    for slug, v in current.items():
        rows.append({
            "slug": slug,
            "stock": (v.get("stock") or "").strip().upper(),
            "url": v.get("url") or "",
            "title": v.get("title") or "",
            "vin": (v.get("vin") or "").strip().upper(),
            "price_int": v.get("price_int"),
            "km_int": v.get("km_int"),
            "status": "ACTIVE",
            "last_seen": now,
            "updated_at": now,
        })
    upsert_inventory(sb, rows)

    # 4) SOLD: disappeared -> update FB text (best-effort)
    for slug in disappeared_slugs:
        post = posts_db.get(slug) or {}
        post_id = post.get("post_id")

        if post_id and str(post.get("status", "")).upper() != "SOLD":
            msg = sold_prefix() + "Ce véhicule est vendu."
            if DRY_RUN:
                print(f"DRY_RUN: would MARK SOLD -> {slug} (post_id={post_id})")
            else:
                try:
                    update_post_text(post_id, FB_TOKEN, msg)
                except Exception as e:
                    log_event(sb, slug, "FB_SOLD_UPDATE_FAIL", {"post_id": post_id, "err": str(e)})

        upsert_post(sb, {
            "slug": slug,
            "post_id": post_id,
            "status": "SOLD",
            "sold_at": now,
            "last_updated_at": now,
        })
        log_event(sb, slug, "SOLD", {"slug": slug, "dry_run": DRY_RUN})

    # 5) PRICE_CHANGED
    price_changed: List[str] = []
    for slug in common_slugs:
        old = inv_db.get(slug) or {}
        new = current.get(slug) or {}
        if (old.get("price_int") is not None) and (new.get("price_int") is not None) and old.get("price_int") != new.get("price_int"):
            price_changed.append(slug)

    # 6) Targets NEW + PRICE_CHANGED
    targets: List[Tuple[str, str]] = [(s, "NEW") for s in new_slugs] + [(s, "PRICE_CHANGED") for s in price_changed]

    # FORCE_PREVIEW by stock
    if FORCE_STOCK:
        forced_slug = None
        for s_slug, v_info in current.items():
            if (v_info.get("stock") or "").strip().upper() == FORCE_STOCK:
                forced_slug = s_slug
                break
        if forced_slug:
            targets = [(forced_slug, "FORCE_PREVIEW")]
            print(f"FORCE_PREVIEW enabled for stock {FORCE_STOCK} -> {forced_slug}")
        else:
            print(f"FORCE_PREVIEW: stock {FORCE_STOCK} introuvable dans l’inventaire courant")
            targets = []  # rien à faire

    # Throttle: limiter le nombre d'actions par run (sauf FORCE)
    if not FORCE_STOCK and MAX_TARGETS > 0:
        targets = targets[:MAX_TARGETS]

    # 7) Process targets
    for slug, event in targets:
        v = current.get(slug) or {}

        # --- Sanitize data (évite km délirants, prix invalides, titres vides) ---
        title_clean = _clean_title(v.get("title") or "")
        if not title_clean:
            log_event(sb, slug, "SKIP_BAD_DATA", {"reason": "title_invalid", "raw_title": v.get("title")})
            continue

        price_int = _clean_price_int(v.get("price_int"))
        km_int = _clean_km(v.get("km_int"))

        vehicle_payload = {
            "title": title_clean,
            "price": (f"{price_int:,}".replace(",", " ") + " $") if price_int else "",
            "mileage": (f"{km_int:,}".replace(",", " ") + " km") if km_int else "",
            "stock": (v.get("stock") or "").strip().upper(),
            "vin": (v.get("vin") or "").strip().upper(),
            "url": v.get("url") or "",
        }

        # Cache sticker (ne bloque jamais le run)
        vin_up = (v.get("vin") or "").strip().upper()
        if _is_stellantis_vin(vin_up):
            try:
                ensure_sticker_cached(sb, vin_up, run_id=run_id)
            except Exception as e:
                log_event(sb, slug, "STICKER_CACHE_FAIL", {"vin": vin_up, "err": str(e), "run_id": run_id})

        fb_text = generate_facebook_text(TEXT_ENGINE_URL, slug=slug, event=event, vehicle=vehicle_payload)
      
        base = (fb_text or "").rstrip()

        # anti-doublon robuste (si le text-engine ajoute déjà une partie)
        markers = ["🔁 J’accepte les échanges", "📞 Daniel Giroux", "#DanielGiroux"]
        if not any(m in base for m in markers):
            fb_text = base + dealer_footer()
        else:
            fb_text = base
      
        post_info = posts_db.get(slug) or {}
        post_id = post_info.get("post_id")

        photo_urls = v.get("photos") or []

        # filtre anti-bannières/promo (crédit-bail, inspectés, etc.)
        bad_kw = (
            "credit", "crédit", "bail", "commercial",
            "inspect", "inspecte", "inspecté", "inspection",
            "garantie", "warranty",
            "finance", "financement",
            "promo", "promotion",
            "banner", "banniere", "bannière",
        )
        photo_urls = [u for u in photo_urls if u and not any(k in u.lower() for k in bad_kw)]

        stock_up = (v.get("stock") or "").strip().upper()
        photo_paths = download_photos(stock_up, photo_urls, limit=MAX_PHOTOS)

        # DRY_RUN: print preview + log event, no FB writes
        if DRY_RUN:
            _preview_text(slug, event, fb_text)
            log_event(sb, slug, event, {"dry_run": True, "photos": len(photo_paths), "post_id": post_id})
            continue

        did_action = False

        # Publish NEW (no post_id) or update PRICE_CHANGED (existing post_id)
        if not post_id:
            main_photos = photo_paths[:POST_PHOTOS]
            extra_photos = photo_paths[POST_PHOTOS:MAX_PHOTOS]

            media_ids = publish_photos_unpublished(FB_PAGE_ID, FB_TOKEN, main_photos, limit=POST_PHOTOS)
            post_id = create_post_with_attached_media(FB_PAGE_ID, FB_TOKEN, fb_text, media_ids)

            if extra_photos:
                try:
                    publish_photos_as_comment_batch(FB_PAGE_ID, FB_TOKEN, post_id, extra_photos)
                except Exception:
                    pass

            upsert_post(sb, {
                "slug": slug,
                "post_id": post_id,
                "status": "ACTIVE",
                "published_at": now,
                "last_updated_at": now,
            })
            log_event(sb, slug, "NEW", {"post_id": post_id, "photos": len(photo_paths), "run_id": run_id})
            did_action = True

        else:
            try:
                update_post_text(post_id, FB_TOKEN, fb_text)
                upsert_post(sb, {
                    "slug": slug,
                    "post_id": post_id,
                    "status": "ACTIVE",
                    "last_updated_at": now,
                })
                log_event(sb, slug, "PRICE_CHANGED", {"post_id": post_id})
                did_action = True
            except Exception:
                pass

        # Throttle: pause après action réelle
        if did_action and SLEEP_BETWEEN > 0:
            time.sleep(SLEEP_BETWEEN)

    print(f"OK: NEW={len(new_slugs)} SOLD={len(disappeared_slugs)} PRICE_CHANGED={len(price_changed)}")

if __name__ == "__main__":
    main()
