import os
import re
import json
import time
import hashlib
import csv
import io
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

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
    # client + core maps
    get_client,
    get_inventory_map,
    get_posts_map,
    upsert_inventory,
    upsert_post,
    log_event,
    utc_now_iso,

    # storage + snapshots
    read_json_from_storage,
    get_latest_snapshot_run_id,
    upload_json_to_storage,
    upload_bytes_to_storage,
    cleanup_storage_runs,

    # mémoire tables
    upsert_scrape_run,
    upsert_raw_page,
    upsert_sticker_pdf,
    upsert_output,
)

# -------------------------
# Env load (local dev only)
# -------------------------
for name in (".env.local", ".kenbot_env", ".env"):
    p = Path(name)
    if p.exists():
        load_dotenv(p, override=False)
        break

# -------------------------
# Config
# -------------------------
BASE_URL = os.getenv("KENBOT_BASE_URL", "https://www.kennebecdodge.ca").rstrip("/")
INVENTORY_PATH = os.getenv("KENBOT_INVENTORY_PATH", "/fr/inventaire-occasion/")

TEXT_ENGINE_URL = (os.getenv("KENBOT_TEXT_ENGINE_URL") or "").strip()

FB_PAGE_ID = (os.getenv("KENBOT_FB_PAGE_ID") or os.getenv("FB_PAGE_ID") or "").strip()
FB_TOKEN = (os.getenv("KENBOT_FB_ACCESS_TOKEN") or os.getenv("FB_PAGE_ACCESS_TOKEN") or "").strip()

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

# Storage buckets
RAW_BUCKET = os.getenv("SB_BUCKET_RAW", "kennebec-raw").strip()
STICKERS_BUCKET = os.getenv("SB_BUCKET_STICKERS", "kennebec-stickers").strip()
SNAP_BUCKET = os.getenv("SB_BUCKET_SNAPSHOTS", "kennebec-facebook-snapshots").strip()
OUTPUTS_BUCKET = os.getenv("SB_BUCKET_OUTPUTS", "kennebec-outputs").strip()

# Behaviour flags
DRY_RUN = os.getenv("KENBOT_DRY_RUN", "0").strip() == "1"
REBUILD_POSTS = os.getenv("KENBOT_REBUILD_POSTS", "0").strip() == "1"
FORCE_STOCK = (os.getenv("KENBOT_FORCE_STOCK") or "").strip().upper()

MAX_TARGETS = int(os.getenv("KENBOT_MAX_TARGETS", "4").strip() or "4")
SLEEP_BETWEEN = int(os.getenv("KENBOT_SLEEP_BETWEEN_POSTS", "30").strip() or "30")

CACHE_STICKERS = os.getenv("KENBOT_CACHE_STICKERS", "1").strip() == "1"
STICKER_MAX = int(os.getenv("KENBOT_STICKER_MAX", "999").strip() or "999")

RAW_KEEP = int(os.getenv("KENBOT_RAW_KEEP", "2").strip() or "2")
SNAP_KEEP = int(os.getenv("KENBOT_SNAP_KEEP", "10").strip() or "10")

MAX_PHOTOS = int(os.getenv("KENBOT_MAX_PHOTOS", "15").strip() or "15")
POST_PHOTOS = int(os.getenv("KENBOT_POST_PHOTOS", "10").strip() or "10")

TMP_PHOTOS = Path(os.getenv("KENBOT_TMP_PHOTOS_DIR", "/tmp/kenbot_photos"))
TMP_PHOTOS.mkdir(parents=True, exist_ok=True)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("🛑 Supabase creds manquants: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY")
if not FB_PAGE_ID or not FB_TOKEN:
    raise SystemExit("🛑 FB creds manquants: KENBOT_FB_PAGE_ID + KENBOT_FB_ACCESS_TOKEN")
if not TEXT_ENGINE_URL:
    raise SystemExit("🛑 KENBOT_TEXT_ENGINE_URL manquant (kenbot-text-engine)")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15",
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
})

# -------------------------
# Helpers
# -------------------------
def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b or b"").hexdigest()

def _run_id_from_now(now_iso: str) -> str:
    digits = "".join(ch for ch in (now_iso or "") if ch.isdigit())
    if len(digits) >= 14:
        return f"{digits[0:8]}_{digits[8:14]}"
    return f"run_{int(time.time())}"

def _is_pdf_ok(b: bytes) -> bool:
    return bool(b) and len(b) >= 10_240 and b[:4] == b"%PDF"

def _is_stellantis_vin(vin: str) -> bool:
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return False
    return vin.startswith(("1C", "2C", "3C", "ZAC", "ZFA"))

def _clean_title(t: str) -> str:
    t = (t or "").strip()
    low = t.lower()
    if low in {"jeep", "dodge", "ram", "chrysler", "fiat"}:
        return ""
    if len(t) < 6:
        return ""
    return t

def _clean_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        s = str(x).replace(" ", "").replace("\u00a0", "").replace(",", "").replace("$", "")
        return int(s)
    except Exception:
        return None

def _dealer_footer() -> str:
    return (
        "\n\n🔁 J’accepte les échanges : 🚗 auto • 🏍️ moto • 🛥️ bateau • 🛻 VTT • 🏁 côte-à-côte\n"
        "📸 Envoie-moi les photos + infos de ton échange (année / km / paiement restant) → je te reviens vite.\n\n"
        "👋 Publiée par Daniel Giroux — je réponds vite (pas un robot, promis 😄)\n"
        "📍 Saint-Georges (Beauce) | Prise de possession rapide possible\n"
        "📄 Vente commerciale — 2 taxes applicables\n\n"
        "📩 Écris-moi en privé\n"
        "📞 Daniel Giroux — 418-222-3939\n\n"
        "#RAM #Truck #Pickup #ProMaster #Cargo #Van #RAM1500 #Beauce #SaintGeorges #Quebec "
        "#AutoUsagée #VehiculeOccasion #DanielGiroux"
    )

FOOTER_MARKERS = [
    "j’accepte", "j'accepte",
    "échange", "echange",
    "financement", "finance",
    "daniel", "giroux",
    "418", "222", "3939",
    "écris-moi", "ecris-moi",
    "en privé", "en prive",
    "#danielgiroux",
]

def ensure_single_footer(text: str, footer: str) -> str:
    """
    Ajoute le footer UNE SEULE FOIS.
    Si le texte contient déjà des marqueurs (footer/cta/téléphone), on n'ajoute rien.
    """
    base = (text or "").rstrip()
    low = base.lower()
    if any(m in low for m in FOOTER_MARKERS):
        return base
    return f"{base}\n\n{footer}".strip()

def _strip_sold_banner(txt: str) -> str:
    t = (txt or "").lstrip()
    if not t.startswith("🚨 VENDU 🚨"):
        return t
    lines = t.splitlines()
    out = []
    cutting = True
    for line in lines:
        if cutting:
            if "────────────────────" in line:
                cutting = False
            continue
        out.append(line)
    return ("\n".join(out)).lstrip()

def _sold_prefix() -> str:
    return (
        "🚨 VENDU 🚨\n\n"
        "Ce véhicule n’est plus disponible.\n\n"
        "👉 Vous recherchez un véhicule semblable ?\n"
        "Contactez-moi directement, je peux vous aider.\n\n"
        "Daniel Giroux\n"
        "📞 418-222-3939\n"
        "────────────────────\n\n"
    )

def _make_sold_message(base_text: str) -> str:
    base = _strip_sold_banner(base_text).strip()
    if not base:
        base = "(Détails indisponibles — contactez-moi.)"
    return _sold_prefix() + base

def _fetch_fb_post_message(post_id: str) -> str:
    url = f"https://graph.facebook.com/v24.0/{post_id}"
    r = SESSION.get(url, params={"fields": "message", "access_token": FB_TOKEN}, timeout=30)
    j = r.json()
    if not r.ok:
        raise RuntimeError(f"FB get post message error: {j}")
    return (j.get("message") or "").strip()

def _download_photo(url: str, out_path: Path) -> None:
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)

def _download_photos(stock: str, urls: List[str], limit: int) -> List[Path]:
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
                _download_photo(u, p)
            except Exception:
                continue
        out.append(p)
    return out

def rebuild_posts_map(limit: int = 300) -> Dict[str, Dict[str, Any]]:
    posts_map: Dict[str, Dict[str, Any]] = {}
    fetched = 0
    after = None

    while fetched < limit:
        params = {"fields": "id,message,created_time,permalink_url", "limit": 25, "access_token": FB_TOKEN}
        if after:
            params["after"] = after

        url = f"https://graph.facebook.com/v24.0/{FB_PAGE_ID}/posts"
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
            if not post_id or not msg:
                continue

            m = re.search(r"\b(\d{5}[A-Za-z]?)\b", msg)
            stock = (m.group(1).upper() if m else "")
            if not stock:
                continue

            posts_map[stock] = {"post_id": post_id, "published_at": created}
            if fetched >= limit:
                break

        paging = (j.get("paging") or {}).get("cursors") or {}
        after = paging.get("after")
        if not after:
            break

    return posts_map

def ensure_sticker_cached(sb, vin: str, run_id: str) -> Dict[str, Any]:
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return {"vin": vin, "status": "skip", "reason": "vin_invalid"}

    ok_path = f"pdf_ok/{vin}.pdf"
    bad_path = f"pdf_bad/{vin}.pdf"

    # Try existing OK
    try:
        blob = sb.storage.from_(STICKERS_BUCKET).download(ok_path)
    except Exception:
        blob = None

    if _is_pdf_ok(blob or b""):
        upsert_sticker_pdf(sb, vin=vin, status="ok", storage_path=ok_path, data=blob, reason="", run_id=run_id)
        return {"vin": vin, "status": "ok"}

    # Try existing BAD
    try:
        blob_bad = sb.storage.from_(STICKERS_BUCKET).download(bad_path)
    except Exception:
        blob_bad = None

    if blob_bad is not None and len(blob_bad) > 0:
        upsert_sticker_pdf(sb, vin=vin, status="bad", storage_path=bad_path, data=blob_bad, reason="cached_bad", run_id=run_id)
        return {"vin": vin, "status": "bad"}

    # Fetch from Stellantis
    pdf_url = f"https://www.chrysler.com/hostd/windowsticker/getWindowStickerPdf.do?vin={vin}"
    try:
        r = SESSION.get(pdf_url, timeout=25)
        fetched = r.content or b""
    except Exception:
        fetched = b""

    if _is_pdf_ok(fetched):
        upload_bytes_to_storage(sb, STICKERS_BUCKET, ok_path, fetched, content_type="application/pdf", upsert=True)
        upsert_sticker_pdf(sb, vin=vin, status="ok", storage_path=ok_path, data=fetched, reason="", run_id=run_id)
        return {"vin": vin, "status": "ok"}

    blob_store = fetched if fetched else b"x"
    upload_bytes_to_storage(sb, STICKERS_BUCKET, bad_path, blob_store, content_type="application/pdf", upsert=True)
    upsert_sticker_pdf(sb, vin=vin, status="bad", storage_path=bad_path, data=blob_store, reason="invalid_pdf", run_id=run_id)
    return {"vin": vin, "status": "bad"}

def build_meta_vehicle_feed_csv(current: dict) -> bytes:
    """
    Génère un feed Meta simple:
    id,title,description,availability,condition,price,link,image_link,brand,year
    """
    fieldnames = ["id","title","description","availability","condition","price","link","image_link","brand","year"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()

    for slug, v in (current or {}).items():
        stock = (v.get("stock") or "").strip().upper()
        url = (v.get("url") or "").strip()
        title = (v.get("title") or "").strip()
        price_int = v.get("price_int")

        # fallback si price_int absent
        if not isinstance(price_int, int):
            p = (v.get("price") or "").strip()
            digits = "".join(ch for ch in p if ch.isdigit())
            price_int = int(digits) if digits else None

        photos = v.get("photos") or []
        image_link = (photos[0] or "").strip() if photos else ""

        if not stock or not url or not title or not isinstance(price_int, int):
            continue
        if not image_link:
            # Meta préfère une image publique; si tu veux, on met no_photo ici plus tard
            continue

        year = ""
        m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
        if m:
            year = m.group(1)

        brand = (v.get("make") or "").strip()
        if not brand:
            brand = title.split(" ", 1)[0].strip()

        w.writerow({
            "id": stock,
            "title": title,
            "description": f"{title} | Stock {stock}",
            "availability": "in stock",
            "condition": "used",
            "price": f"{price_int} CAD",
            "link": url,
            "image_link": image_link,
            "brand": brand,
            "year": year,
        })

    return buf.getvalue().encode("utf-8")

# -------------------------
# Main
# -------------------------
def main() -> None:
    sb = get_client(SUPABASE_URL, SUPABASE_KEY)
    now = utc_now_iso()
    run_id = _run_id_from_now(now)

    inv_db = get_inventory_map(sb)
    posts_db = get_posts_map(sb)

    # Optional rebuild FB posts
    fb_map: Dict[str, Dict[str, Any]] = {}
    if REBUILD_POSTS:
        fb_map = rebuild_posts_map(limit=300)
        upload_json_to_storage(sb, SNAP_BUCKET, f"runs/{run_id}/fb_map_by_stock.json", fb_map, upsert=True)

        updated = 0
        for slug, inv in inv_db.items():
            stock = (inv.get("stock") or "").strip().upper()
            info = fb_map.get(stock) if stock else None
            if not info:
                continue
            upsert_post(sb, {
                "slug": slug,
                "post_id": info.get("post_id"),
                "status": "ACTIVE",
                "published_at": info.get("published_at"),
                "last_updated_at": now,
                "stock": stock,
            })
            updated += 1

        log_event(sb, "REBUILD", "REBUILD_POSTS_OK", {"fb_found": len(fb_map), "updated": updated, "run_id": run_id})
        posts_db = get_posts_map(sb)

    # Fetch 3 listing pages (RAW)
    pages = [
        f"{BASE_URL}{INVENTORY_PATH}",
        f"{BASE_URL}{INVENTORY_PATH}?page=2",
        f"{BASE_URL}{INVENTORY_PATH}?page=3",
    ]

    pages_html: List[Tuple[int, str]] = []
    all_urls: List[str] = []

    for idx, page_url in enumerate(pages, start=1):
        try:
            html = SESSION.get(page_url, timeout=30).text
        except Exception as e:
            html = ""
            log_event(sb, "SCRAPE", "PAGE_FETCH_FAIL", {"page": page_url, "err": str(e), "run_id": run_id})

        pages_html.append((idx, html))
        if html:
            try:
                all_urls += parse_inventory_listing_urls(BASE_URL, INVENTORY_PATH, html)
            except Exception as e:
                log_event(sb, "SCRAPE", "PARSE_LISTING_FAIL", {"page": page_url, "err": str(e), "run_id": run_id})

    all_urls = sorted(list(dict.fromkeys(all_urls)))

    # Upload RAW pages + DB raw_pages
    meta = {
        "run_id": run_id,
        "ts": now,
        "pages": pages,
        "urls_count": len(all_urls),
        "dry_run": DRY_RUN,
        "cache_stickers": CACHE_STICKERS,
        "sticker_max": STICKER_MAX,
    }
    upload_json_to_storage(sb, RAW_BUCKET, f"raw_pages/{run_id}/meta.json", meta, upsert=True)

    for page_no, html in pages_html:
        data = (html or "").encode("utf-8")
        storage_path = f"raw_pages/{run_id}/kennebec_page_{page_no}.html"
        upload_bytes_to_storage(sb, RAW_BUCKET, storage_path, data, content_type="text/html; charset=utf-8", upsert=True)
        try:
            upsert_raw_page(sb, run_id, page_no, storage_path, data)
        except Exception as e:
            log_event(sb, "RAW", "RAW_PAGE_DB_FAIL", {"page_no": page_no, "err": str(e), "run_id": run_id})

    try:
        deleted = cleanup_storage_runs(sb, RAW_BUCKET, "raw_pages", keep=RAW_KEEP)
        log_event(sb, "RAW", "RAW_CLEANUP", {"keep": RAW_KEEP, "deleted": deleted, "run_id": run_id})
    except Exception as e:
        log_event(sb, "RAW", "RAW_CLEANUP_FAIL", {"err": str(e), "run_id": run_id})

    # Parse inventory vehicles
    current: Dict[str, Dict[str, Any]] = {}
    for url in all_urls:
        try:
            d = parse_vehicle_detail_simple(SESSION, url)
        except Exception as e:
            log_event(sb, "SCRAPE", "DETAIL_FAIL", {"url": url, "err": str(e), "run_id": run_id})
            continue

        stock = (d.get("stock") or "").strip().upper()
        title = _clean_title(d.get("title") or "")
        if not stock or not title:
            continue

        d["title"] = title
        d["stock"] = stock
        d["url"] = d.get("url") or url
        d["vin"] = (d.get("vin") or "").strip().upper()
        d["price_int"] = _clean_int(d.get("price_int"))
        d["km_int"] = _clean_int(d.get("km_int"))

        slug = slugify(title, stock)
        d["slug"] = slug
        current[slug] = d

    inv_count = len(current)
    upsert_scrape_run(sb, run_id, status="OK", note=f"inv_count={inv_count} urls={len(all_urls)}")

    meta["inventory_count"] = inv_count
    upload_json_to_storage(sb, RAW_BUCKET, f"raw_pages/{run_id}/meta.json", meta, upsert=True)

    # Cache stickers for ALL inventory
    vin_status: Dict[str, str] = {}
    if CACHE_STICKERS:
        vins = []
        for v in current.values():
            vin = (v.get("vin") or "").strip().upper()
            if _is_stellantis_vin(vin):
                vins.append(vin)
        vins = list(dict.fromkeys(vins))[:max(0, STICKER_MAX)]

        ok = bad = skip = 0
        for vin in vins:
            try:
                res = ensure_sticker_cached(sb, vin, run_id)
                st = (res.get("status") or "").lower()
                vin_status[vin] = st
                if st == "ok":
                    ok += 1
                elif st == "bad":
                    bad += 1
                else:
                    skip += 1
            except Exception as e:
                log_event(sb, "STICKER", "STICKER_FAIL", {"vin": vin, "err": str(e), "run_id": run_id})

        log_event(sb, "STICKER", "STICKER_SUMMARY", {"ok": ok, "bad": bad, "skip": skip, "total": len(vins), "run_id": run_id})

    # Upsert inventory ACTIVE
    rows = []
    for slug, v in current.items():
        rows.append({
            "slug": slug,
            "stock": v.get("stock"),
            "url": v.get("url"),
            "title": v.get("title"),
            "vin": v.get("vin"),
            "price_int": v.get("price_int"),
            "km_int": v.get("km_int"),
            "status": "ACTIVE",
            "last_seen": now,
            "updated_at": now,
        })
    upsert_inventory(sb, rows)

    # SOLD detection
    current_slugs = set(current.keys())
    inv_db_active = {slug: r for slug, r in inv_db.items() if (r.get("status") or "").upper() == "ACTIVE"}
    db_slugs = set(inv_db_active.keys())

    disappeared_slugs = sorted(db_slugs - current_slugs)
    new_slugs = sorted(current_slugs - db_slugs)
    common_slugs = sorted(current_slugs & db_slugs)

    # SOLD flow
    for slug in disappeared_slugs:
        post = posts_db.get(slug) or {}
        post_id = post.get("post_id")

        if post_id and str(post.get("status", "")).upper() != "SOLD":
            if DRY_RUN:
                print(f"DRY_RUN: would MARK SOLD -> {slug} (post_id={post_id})")
            else:
                try:
                    base_text = (post.get("base_text") or "").strip()
                    if not base_text:
                        fb_current = _fetch_fb_post_message(post_id)
                        base_text = _strip_sold_banner(fb_current)

                    msg = _make_sold_message(base_text)
                    update_post_text(post_id, FB_TOKEN, msg)

                    upsert_post(sb, {
                        "slug": slug,
                        "post_id": post_id,
                        "status": "SOLD",
                        "sold_at": now,
                        "last_updated_at": now,
                        "base_text": base_text,
                        "stock": post.get("stock"),
                    })
                    log_event(sb, slug, "SOLD", {"post_id": post_id, "run_id": run_id})
                except Exception as e:
                    log_event(sb, slug, "FB_SOLD_FAIL", {"post_id": post_id, "err": str(e), "run_id": run_id})

        old_inv = inv_db.get(slug) or {}
        upsert_inventory(sb, [{
            "slug": slug,
            "stock": old_inv.get("stock"),
            "url": old_inv.get("url"),
            "title": old_inv.get("title"),
            "vin": old_inv.get("vin"),
            "price_int": old_inv.get("price_int"),
            "km_int": old_inv.get("km_int"),
            "status": "SOLD",
            "last_seen": old_inv.get("last_seen") or now,
            "updated_at": now,
        }])

    # PRICE_CHANGED
    price_changed: List[str] = []
    for slug in common_slugs:
        old = inv_db.get(slug) or {}
        new = current.get(slug) or {}
        if old.get("price_int") is not None and new.get("price_int") is not None and old.get("price_int") != new.get("price_int"):
            price_changed.append(slug)

    if os.getenv("KENBOT_BUILD_META_FEEDS", "0").strip() == "1":
        feed_bytes = build_meta_vehicle_feed_csv(current)
        upload_bytes_to_storage(
            sb,
            OUTPUTS_BUCKET,
            "feeds/meta_vehicle.csv",
            feed_bytes,
            content_type="text/csv; charset=utf-8",
            upsert=True,
        )
        upload_bytes_to_storage(
            sb,
            OUTPUTS_BUCKET,
            f"runs/{run_id}/feeds/meta_vehicle.csv",
            feed_bytes,
            content_type="text/csv; charset=utf-8",
            upsert=True,
        )
        print("✅ Uploaded Meta feed -> kennebec-outputs/feeds/meta_vehicle.csv", flush=True)

    # Snapshots index_by_stock
    if not fb_map:
        latest_run = get_latest_snapshot_run_id(sb, SNAP_BUCKET)
        if latest_run:
            fb_map = read_json_from_storage(sb, SNAP_BUCKET, f"runs/{latest_run}/fb_map_by_stock.json") or {}

    current_by_stock: Dict[str, str] = {}
    for s_slug, v in current.items():
        st = (v.get("stock") or "").strip().upper()
        if st:
            current_by_stock[st] = s_slug

    index_by_stock: Dict[str, Any] = {}
    for stock, info in (fb_map or {}).items():
        s = (stock or "").strip().upper()
        slug = current_by_stock.get(s)
        v = current.get(slug) if slug else None
        index_by_stock[s] = {
            "post_id": (info or {}).get("post_id"),
            "published_at": (info or {}).get("published_at"),
            "kennebec_price_int": (v.get("price_int") if v else None),
            "kennebec_title": (v.get("title") if v else None),
            "kennebec_url": (v.get("url") if v else None),
        }

    upload_json_to_storage(sb, SNAP_BUCKET, f"runs/{run_id}/index_by_stock.json", index_by_stock, upsert=True)
    try:
        cleanup_storage_runs(sb, SNAP_BUCKET, "runs", keep=SNAP_KEEP)
    except Exception:
        pass

    # Targets
    targets: List[Tuple[str, str]] = [(s, "PRICE_CHANGED") for s in price_changed] + [(s, "NEW") for s in new_slugs]

    # Flags
    BUILD_ALL_OUTPUTS = os.getenv("KENBOT_BUILD_ALL_OUTPUTS", "0").strip() == "1"
    PUBLISH_MISSING = os.getenv("KENBOT_PUBLISH_MISSING", "0").strip() == "1"

    # Build pdf_ok_vins from storage (source of truth)
    STICKER_BUCKET = os.getenv("KENBOT_STICKER_BUCKET", "kennebec-stickers").strip()
    pdf_ok_vins: set[str] = set()
    try:
        for o in (sb.storage.from_(STICKER_BUCKET).list("pdf_ok") or []):
            name = (o.get("name") or "")
            if name.lower().endswith(".pdf"):
                pdf_ok_vins.add(name[:-4].upper())  # strip .pdf
        print(f"STICKERS pdf_ok_vins={len(pdf_ok_vins)}", flush=True)
    except Exception as e:
        print(f"⚠️ Cannot list {STICKER_BUCKET}/pdf_ok: {e}", flush=True)

    # FORCE_STOCK (priorité #1)
    if FORCE_STOCK:
        forced_slug = None
        for s_slug, v_info in current.items():
            if (v_info.get("stock") or "").strip().upper() == FORCE_STOCK:
                forced_slug = s_slug
                break
        targets = [(forced_slug, "FORCE_PREVIEW")] if forced_slug else []

    # BUILD_ALL_OUTPUTS (priorité #2)
    elif BUILD_ALL_OUTPUTS:
        targets = [(s, "BUILD_OUTPUT") for s in current.keys()]
        print(f"BUILD_ALL_OUTPUTS enabled: targets={len(targets)}", flush=True)

    # PUBLISH_MISSING (priorité #3)
    elif PUBLISH_MISSING:
        missing = []
        for s_slug in current.keys():
            info = posts_db.get(s_slug) or {}
            if not info.get("post_id"):
                missing.append(s_slug)
        targets = [(s, "MISSING_POST") for s in missing]
        print(f"PUBLISH_MISSING enabled: missing_posts={len(missing)}", flush=True)

    # Limite targets (mais PAS en BUILD_ALL_OUTPUTS)
    if not FORCE_STOCK and MAX_TARGETS > 0 and not BUILD_ALL_OUTPUTS:
        targets = targets[:MAX_TARGETS]

    # Process targets
    for slug, event in targets:
        v = current.get(slug) or {}
        stock = (v.get("stock") or "").strip().upper()
        vin = (v.get("vin") or "").strip().upper()
        title = _clean_title(v.get("title") or "")
        if not stock or not title:
            log_event(sb, slug, "SKIP_BAD_DATA", {"reason": "missing_stock_or_title", "run_id": run_id})
            continue

        price_int = _clean_int(v.get("price_int"))
        km_int = _clean_int(v.get("km_int"))

        vehicle_payload = {
            "title": title,
            "price": (f"{price_int:,}".replace(",", " ") + " $") if price_int else "",
            "mileage": (f"{km_int:,}".replace(",", " ") + " km") if km_int else "",
            "stock": stock,
            "vin": vin,
            "url": v.get("url") or "",
        }

        fb_text = ensure_single_footer(
            generate_facebook_text(TEXT_ENGINE_URL, slug=slug, event=event, vehicle=vehicle_payload),
            _dealer_footer(),
        )

        # with/without = pdf_ok (source of truth)
        out_folder = "with" if (vin and vin in pdf_ok_vins) else "without"
        fb_out_path = f"{out_folder}/{stock}_facebook.txt"
        mp_out_path = f"{out_folder}/{stock}_marketplace.txt"

        upload_bytes_to_storage(
            sb, OUTPUTS_BUCKET, fb_out_path, (fb_text + "\n").encode("utf-8"),
            content_type="text/plain; charset=utf-8", upsert=True
        )
        upload_bytes_to_storage(
            sb, OUTPUTS_BUCKET, mp_out_path, (fb_text + "\n").encode("utf-8"),
            content_type="text/plain; charset=utf-8", upsert=True
        )
        upsert_output(sb, stock=stock, kind="text", facebook_path=fb_out_path, marketplace_path=mp_out_path, run_id=run_id)

        post_info = posts_db.get(slug) or {}
        post_id = post_info.get("post_id")

        photo_urls = v.get("photos") or []
        bad_kw = ("credit", "crédit", "bail", "commercial", "inspect", "inspection", "garantie",
                  "warranty", "finance", "financement", "promo", "promotion", "banner", "banniere", "bannière")
        photo_urls = [u for u in photo_urls if u and not any(k in u.lower() for k in bad_kw)]
        photo_paths = _download_photos(stock, photo_urls, limit=MAX_PHOTOS)

        ALLOW_NO_PHOTO = os.getenv("KENBOT_ALLOW_NO_PHOTO", "0").strip() == "1"
        NO_PHOTO_BUCKET = (os.getenv("KENBOT_NO_PHOTO_BUCKET") or OUTPUTS_BUCKET).strip()
        NO_PHOTO_PATH = (os.getenv("KENBOT_NO_PHOTO_PATH") or "assets/no_photo.png").strip()

        if not photo_paths:
            log_event(sb, slug, "NO_PHOTOS", {"stock": stock, "url": v.get("url"), "run_id": run_id})

            if not ALLOW_NO_PHOTO:
                print(f"SKIP {stock}: no photos (set KENBOT_ALLOW_NO_PHOTO=1)", flush=True)
                continue

            try:
                blob = sb.storage.from_(NO_PHOTO_BUCKET).download(NO_PHOTO_PATH)
            except Exception as e:
                print(
                    f"SKIP {stock}: cannot download placeholder {NO_PHOTO_BUCKET}/{NO_PHOTO_PATH} -> {e}",
                    flush=True,
                )
                continue

            tmp_placeholder = Path("/tmp") / "kenbot_no_photo.png"
            tmp_placeholder.write_bytes(blob)

            photo_paths = [tmp_placeholder]
            #fb_text = "📷 Photos suivront bientôt.\n\n" + fb_text
    
        if DRY_RUN:
            print(f"\n=== DRY_RUN {event}: {slug} ({stock}) ===\n{fb_text[:900]}\n")
            log_event(sb, slug, event, {"dry_run": True, "photos": len(photo_paths), "post_id": post_id, "run_id": run_id})
            continue

        if event == "PRICE_CHANGED" and not post_id:
            log_event(sb, slug, "PRICE_CHANGED_SKIP_NO_POST_ID", {"run_id": run_id})
            continue

        if not post_id:
            main_photos = photo_paths[:POST_PHOTOS]
            extra_photos = photo_paths[POST_PHOTOS:MAX_PHOTOS]
            try:
                media_ids = publish_photos_unpublished(FB_PAGE_ID, FB_TOKEN, main_photos, limit=POST_PHOTOS)
                post_id = create_post_with_attached_media(FB_PAGE_ID, FB_TOKEN, fb_text, media_ids)
                if extra_photos:
                    publish_photos_as_comment_batch(FB_PAGE_ID, FB_TOKEN, post_id, extra_photos)

                upsert_post(sb, {
                    "slug": slug,
                    "post_id": post_id,
                    "status": "ACTIVE",
                    "published_at": now,
                    "last_updated_at": now,
                    "base_text": fb_text,
                    "stock": stock,
                })
                log_event(sb, slug, "FB_NEW_OK", {"post_id": post_id, "photos": len(photo_paths), "run_id": run_id})
            except Exception as e:
                log_event(sb, slug, "FB_NEW_FAIL", {"err": str(e), "run_id": run_id})
        else:
            try:
                update_post_text(post_id, FB_TOKEN, fb_text)
                upsert_post(sb, {
                    "slug": slug,
                    "post_id": post_id,
                    "status": "ACTIVE",
                    "last_updated_at": now,
                    "base_text": fb_text,
                    "stock": stock,
                })
                log_event(sb, slug, "FB_UPDATE_OK", {"post_id": post_id, "event": event, "run_id": run_id})
            except Exception as e:
                log_event(sb, slug, "FB_UPDATE_FAIL", {"post_id": post_id, "err": str(e), "run_id": run_id})

        if SLEEP_BETWEEN > 0:
            time.sleep(SLEEP_BETWEEN)

    print(f"OK run_id={run_id} inv_count={inv_count} NEW={len(new_slugs)} SOLD={len(disappeared_slugs)} PRICE_CHANGED={len(price_changed)}")


if __name__ == "__main__":
    main()
