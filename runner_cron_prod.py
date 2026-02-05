#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
runner_cron_prod.py (v2 PROD) - FIXED
FIXES:
- Slug stable par STOCK: réutilise le slug DB quand stock déjà connu (évite duplicats inventory ACTIVE/SOLD)
- Upserts de statut incluent toujours stock quand possible (RECOVERED/MISSING/SOLD)
- daily_audit_and_fix lit inventory ACTIVE "meilleur row par stock" (pas de doublons contradictoires)
"""

import os
import re
import io
import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from kennebec_scrape import (
    fetch_html,
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
    upload_json_to_storage,
    upload_bytes_to_storage,
    cleanup_storage_runs,
    upsert_scrape_run,
    upsert_raw_page,
    upsert_sticker_pdf,
)

from sticker_to_ad import extract_spans_pdfminer, extract_option_groups_from_spans
from ad_builder import build_ad as build_ad_from_options


# -------------------------
# Env load (local dev)
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
INVENTORY_PATH = os.getenv("KENBOT_INVENTORY_PATH", "/fr/inventaire-occasion/").strip()

TEXT_ENGINE_URL = (os.getenv("KENBOT_TEXT_ENGINE_URL") or "").strip()

FB_PAGE_ID = (os.getenv("KENBOT_FB_PAGE_ID") or os.getenv("FB_PAGE_ID") or "").strip()
FB_TOKEN = (os.getenv("KENBOT_FB_ACCESS_TOKEN") or os.getenv("FB_PAGE_ACCESS_TOKEN") or "").strip()

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

RAW_BUCKET = os.getenv("SB_BUCKET_RAW", "kennebec-raw").strip()
STICKERS_BUCKET = os.getenv("SB_BUCKET_STICKERS", "kennebec-stickers").strip()
SNAP_BUCKET = os.getenv("SB_BUCKET_SNAPSHOTS", "kennebec-facebook-snapshots").strip()
OUTPUTS_BUCKET = os.getenv("SB_BUCKET_OUTPUTS", "kennebec-outputs").strip()

DRY_RUN = os.getenv("KENBOT_DRY_RUN", "0").strip() == "1"
RUN_MODE = os.getenv("KENBOT_RUN_MODE", "FULL").strip().upper()  # FULL | FB

MAX_TARGETS = int(os.getenv("KENBOT_MAX_TARGETS", "4").strip() or "4")
SLEEP_BETWEEN = int(os.getenv("KENBOT_SLEEP_BETWEEN_POSTS", "30").strip() or "30")

MAX_PHOTOS = int(os.getenv("KENBOT_MAX_PHOTOS", "15").strip() or "15")
POST_PHOTOS = int(os.getenv("KENBOT_POST_PHOTOS", "10").strip() or "10")

CACHE_STICKERS = os.getenv("KENBOT_CACHE_STICKERS", "1").strip() == "1"
STICKER_MAX = int(os.getenv("KENBOT_STICKER_MAX", "999").strip() or "999")

RAW_KEEP = int(os.getenv("KENBOT_RAW_KEEP", "2").strip() or "2")
SNAP_KEEP = int(os.getenv("KENBOT_SNAP_KEEP", "10").strip() or "10")
OUTPUT_RUNS_KEEP = int(os.getenv("KENBOT_OUTPUT_RUNS_KEEP", "5").strip() or "5")

# Anti faux-vendu
MIN_INVENTORY_ABS = int(os.getenv("KENBOT_MIN_INVENTORY_ABS", "30").strip() or "30")
MIN_INVENTORY_RATIO = float(os.getenv("KENBOT_MIN_INVENTORY_RATIO", "0.70").strip() or "0.70")

# StickerToAd Facebook only
USE_STICKER_AD = os.getenv("KENBOT_FB_USE_STICKER_AD", "1").strip() == "1"

# No photo legacy
ALLOW_NO_PHOTO = os.getenv("KENBOT_ALLOW_NO_PHOTO", "1").strip() == "1"
NO_PHOTO_URL = (os.getenv("KENBOT_NO_PHOTO_URL") or "").strip()

if not NO_PHOTO_URL:
    nb = (os.getenv("KENBOT_NO_PHOTO_BUCKET") or "").strip()
    np = (os.getenv("KENBOT_NO_PHOTO_PATH") or "").strip().lstrip("/")
    sb_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    if nb and np and sb_url:
        NO_PHOTO_URL = f"{sb_url}/storage/v1/object/public/{nb}/{np}"

# Reports
BUILD_META_FEEDS = os.getenv("KENBOT_BUILD_META_FEEDS", "0").strip() == "1"
COMPARE_META_VS_SITE = os.getenv("KENBOT_COMPARE_META_VS_SITE", "0").strip() == "1"
COMPARE_META_LIMIT = int(os.getenv("KENBOT_COMPARE_META_LIMIT", "30").strip() or "30")

# Daily refresh no_photo (FULL only)
REFRESH_NO_PHOTO_DAILY = os.getenv("KENBOT_REFRESH_NO_PHOTO_DAILY", "1").strip() == "1"
REFRESH_NO_PHOTO_LIMIT = int(os.getenv("KENBOT_REFRESH_NO_PHOTO_LIMIT", "25").strip() or "25")

# Daily audit+fix (FULL only)
DAILY_FIX = os.getenv("KENBOT_DAILY_FIX", "1").strip() == "1"
DAILY_FIX_LIMIT = int(os.getenv("KENBOT_DAILY_FIX_LIMIT", "120").strip() or "120")
DAILY_FIX_SLEEP = int(os.getenv("KENBOT_DAILY_FIX_SLEEP", "12").strip() or "12")
DAILY_FIX_FALLBACK = os.getenv("KENBOT_DAILY_FIX_FALLBACK", "1").strip() == "1"

# Lock anti-overlap (2 crons/jour)
LOCK_TTL_SEC = int(os.getenv("KENBOT_LOCK_TTL_SEC", str(45 * 60)).strip() or str(45 * 60))
LOCK_PATH = os.getenv("KENBOT_LOCK_PATH", "locks/runner.lock").strip()

TMP_PHOTOS = Path(os.getenv("KENBOT_TMP_PHOTOS_DIR", "/tmp/kenbot_photos"))
TMP_PHOTOS.mkdir(parents=True, exist_ok=True)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("🛑 Supabase creds manquants: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY")
if not FB_PAGE_ID or not FB_TOKEN:
    raise SystemExit("🛑 FB creds manquants: KENBOT_FB_PAGE_ID + KENBOT_FB_ACCESS_TOKEN")
if not TEXT_ENGINE_URL:
    raise SystemExit("🛑 KENBOT_TEXT_ENGINE_URL manquant")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (KenBot runner_cron_prod)",
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
})


# -------------------------
# Helpers
# -------------------------
def _run_id_from_now(now_iso: str) -> str:
    digits = "".join(ch for ch in (now_iso or "") if ch.isdigit())
    if len(digits) >= 14:
        return f"{digits[0:8]}_{digits[8:14]}"
    return f"run_{int(time.time())}"

def _is_pdf_ok(b: bytes) -> bool:
    return bool(b) and len(b) >= 10_240 and b[:4] == b"%PDF"

def _is_stellantis_vin(vin: str) -> bool:
    vin = (vin or "").strip().upper()
    return len(vin) == 17 and vin.startswith(("1C", "2C", "3C", "ZAC", "ZFA"))

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

def acquire_lock_or_exit(sb) -> None:
    now = int(time.time())
    try:
        b = sb.storage.from_(OUTPUTS_BUCKET).download(LOCK_PATH)
        if b:
            data = json.loads(b.decode("utf-8", "ignore"))
            ts = int(data.get("ts", 0))
            if (now - ts) < LOCK_TTL_SEC:
                print("🔒 LOCK: un autre run est en cours → exit", flush=True)
                raise SystemExit(0)
    except Exception:
        pass

    payload = json.dumps({"ts": now, "mode": RUN_MODE}).encode("utf-8")
    sb.storage.from_(OUTPUTS_BUCKET).upload(
        LOCK_PATH,
        payload,
        {"content-type": "application/json", "x-upsert": "true"},
    )

def release_lock(sb) -> None:
    try:
        sb.storage.from_(OUTPUTS_BUCKET).remove([LOCK_PATH])
    except Exception:
        pass

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
        if not u:
            continue
        ext = ".jpg"
        low = u.lower()
        if ".png" in low:
            ext = ".png"
        elif ".webp" in low:
            ext = ".webp"
        p = folder / f"{stock}_{i:02d}{ext}"
        try:
            _download_photo(u, p)
            out.append(p)
        except Exception:
            continue
    return out

def ensure_sticker_cached(sb, vin: str, run_id: str) -> Dict[str, Any]:
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return {"vin": vin, "status": "skip", "reason": "vin_invalid"}

    ok_path = f"pdf_ok/{vin}.pdf"
    bad_path = f"pdf_bad/{vin}.pdf"

    try:
        blob = sb.storage.from_(STICKERS_BUCKET).download(ok_path)
    except Exception:
        blob = None

    if _is_pdf_ok(blob or b""):
        upsert_sticker_pdf(sb, vin=vin, status="ok", storage_path=ok_path, data=blob, reason="", run_id=run_id)
        return {"vin": vin, "status": "ok", "path": ok_path}

    try:
        blob_bad = sb.storage.from_(STICKERS_BUCKET).download(bad_path)
    except Exception:
        blob_bad = None

    if blob_bad is not None and len(blob_bad) > 0:
        upsert_sticker_pdf(sb, vin=vin, status="bad", storage_path=bad_path, data=blob_bad, reason="cached_bad", run_id=run_id)
        return {"vin": vin, "status": "bad", "path": bad_path}

    pdf_url = f"https://www.chrysler.com/hostd/windowsticker/getWindowStickerPdf.do?vin={vin}"
    try:
        r = SESSION.get(pdf_url, timeout=25)
        fetched = r.content or b""
    except Exception:
        fetched = b""

    if _is_pdf_ok(fetched):
        upload_bytes_to_storage(sb, STICKERS_BUCKET, ok_path, fetched, content_type="application/pdf", upsert=True)
        upsert_sticker_pdf(sb, vin=vin, status="ok", storage_path=ok_path, data=fetched, reason="", run_id=run_id)
        return {"vin": vin, "status": "ok", "path": ok_path}

    blob_store = fetched if fetched else b"x"
    upload_bytes_to_storage(sb, STICKERS_BUCKET, bad_path, blob_store, content_type="application/pdf", upsert=True)
    upsert_sticker_pdf(sb, vin=vin, status="bad", storage_path=bad_path, data=blob_store, reason="invalid_pdf", run_id=run_id)
    return {"vin": vin, "status": "bad", "path": bad_path}

def _extract_options_from_sticker_bytes(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    if not _is_pdf_ok(pdf_bytes):
        return []
    tmp = Path("/tmp") / f"sticker_{int(time.time())}.pdf"
    tmp.write_bytes(pdf_bytes)
    try:
        spans = extract_spans_pdfminer(tmp, max_pages=2)
        groups = extract_option_groups_from_spans(spans) or []
        return [g for g in groups if (g.get("title") or "").strip()]
    except Exception:
        return []
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

def _today_key_utc() -> str:
    ts = time.gmtime()
    return f"{ts.tm_year:04d}{ts.tm_mon:02d}{ts.tm_mday:02d}"

def _try_daily_guard(sb, prefix: str) -> bool:
    path = f"locks/{prefix}_{_today_key_utc()}.lock"
    try:
        b = sb.storage.from_(OUTPUTS_BUCKET).download(path)
        if b:
            return False
    except Exception:
        pass

    payload = json.dumps({"ts": int(time.time()), "prefix": prefix}).encode("utf-8")
    sb.storage.from_(OUTPUTS_BUCKET).upload(
        path,
        payload,
        {"content-type": "application/json", "x-upsert": "true"},
    )
    return True

def _best_row_per_stock(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    rank = {"ACTIVE": 3, "MISSING": 2, "SOLD": 1}
    tmp: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows or []:
        st = (r.get("stock") or "").strip().upper()
        if not st:
            continue
        tmp.setdefault(st, []).append(r)

    out: Dict[str, Dict[str, Any]] = {}
    def key(r):
        st = (r.get("status") or "").upper()
        return (rank.get(st, 0), (r.get("updated_at") or ""), (r.get("last_seen") or ""))
    for st, lst in tmp.items():
        out[st] = sorted(lst, key=key, reverse=True)[0]
    return out

def _build_ad_text(sb, run_id: str, slug: str, v: Dict[str, Any], event: str) -> str:
    vin = (v.get("vin") or "").strip().upper()
    stock = (v.get("stock") or "").strip().upper()
    title = (v.get("title") or "").strip()
    url = (v.get("url") or "").strip()

    price = (v.get("price") or "").strip()
    if not price and isinstance(v.get("price_int"), int):
        price = f"{v['price_int']} $"

    mileage = (v.get("mileage") or v.get("km") or "").strip()
    if not mileage and isinstance(v.get("km_int"), int):
        mileage = f"{v['km_int']} km"

    if USE_STICKER_AD and _is_stellantis_vin(vin):
        try:
            res = ensure_sticker_cached(sb, vin, run_id)
            if (res.get("status") or "").lower() == "ok":
                pdf_path = res.get("path") or f"pdf_ok/{vin}.pdf"
                pdf_bytes = sb.storage.from_(STICKERS_BUCKET).download(pdf_path)
                options = _extract_options_from_sticker_bytes(pdf_bytes)
                if options:
                    txt = build_ad_from_options(
                        title=title,
                        price=price,
                        mileage=mileage,
                        stock=stock,
                        vin=vin,
                        options=options,
                        vehicle_url=url,
                    )
                    print(f"STICKER_TO_AD: USED vin={vin} stock={stock}", flush=True)
                    return txt
        except Exception as e:
            print(f"STICKER_TO_AD: FAIL vin={vin} stock={stock} err={e}", flush=True)

    return generate_facebook_text(TEXT_ENGINE_URL, slug, event, v)

def daily_audit_and_fix(sb, run_id: str) -> dict:
    if RUN_MODE != "FULL":
        return {"skipped": "not_full"}
    if not DAILY_FIX:
        return {"skipped": "disabled"}
    if not _try_daily_guard(sb, "daily_fix"):
        return {"skipped": "already_done_today"}

    inv_rows = (
        sb.table("inventory")
        .select("stock,slug,title,url,vin,price_int,km_int,status,updated_at,last_seen")
        .eq("status", "ACTIVE")
        .limit(5000)
        .execute()
        .data
        or []
    )
    site_by_stock = _best_row_per_stock(inv_rows)
    site_stocks = sorted(site_by_stock.keys())

    posts_rows = (
        sb.table("posts")
        .select("post_id,stock,slug,status,base_text,last_updated_at,sold_at")
        .neq("post_id", None)
        .limit(5000)
        .execute()
        .data
        or []
    )

    posts_by_stock = {}
    for p in posts_rows:
        st = (p.get("stock") or "").strip().upper()
        if not st:
            continue
        cur = posts_by_stock.get(st)
        if not cur or (p.get("last_updated_at") or "") > (cur.get("last_updated_at") or ""):
            posts_by_stock[st] = p

    restored = 0
    text_fixed = 0
    checked = 0
    missing_post = 0

    for stock in site_stocks[:DAILY_FIX_LIMIT]:
        site = site_by_stock.get(stock) or {}
        p = posts_by_stock.get(stock)

        if not p or not p.get("post_id"):
            missing_post += 1
            continue

        post_id = p["post_id"]
        fb_status = (p.get("status") or "").upper()
        base_text = (p.get("base_text") or "").strip()
        has_sold_banner = base_text.lstrip().startswith("🚨 VENDU 🚨")

        if fb_status == "SOLD" or has_sold_banner:
            restore_text = _strip_sold_banner(base_text) if base_text else ""
            if not restore_text:
                payload = {
                    "slug": (site.get("slug") or p.get("slug") or "").strip(),
                    "stock": stock,
                    "title": (site.get("title") or "").strip(),
                    "url": (site.get("url") or "").strip(),
                    "vin": (site.get("vin") or "").strip(),
                    "price_int": site.get("price_int"),
                    "km_int": site.get("km_int"),
                }
                restore_text = _build_ad_text(sb, run_id, payload["slug"] or stock, payload, event="PRICE_CHANGED")

            if not DRY_RUN:
                try:
                    update_post_text(post_id, FB_TOKEN, restore_text)
                    sb.table("posts").update({
                        "status": "ACTIVE",
                        "sold_at": None,
                        "last_updated_at": utc_now_iso(),
                        "base_text": restore_text,
                    }).eq("post_id", post_id).execute()
                    time.sleep(max(2, DAILY_FIX_SLEEP))
                except Exception as e:
                    log_event(sb, stock, "DAILY_RESTORE_FAIL", {"post_id": post_id, "err": str(e), "run_id": run_id})
            restored += 1

        vin = (site.get("vin") or "").strip().upper()
        has_pdf_ok = False
        if vin and len(vin) == 17:
            try:
                pdf = sb.storage.from_(STICKERS_BUCKET).download(f"pdf_ok/{vin}.pdf")
                has_pdf_ok = bool(pdf) and pdf[:4] == b"%PDF"
            except Exception:
                has_pdf_ok = False

        payload = {
            "slug": (site.get("slug") or p.get("slug") or "").strip(),
            "stock": stock,
            "title": (site.get("title") or "").strip(),
            "url": (site.get("url") or "").strip(),
            "vin": vin,
            "price_int": site.get("price_int"),
            "km_int": site.get("km_int"),
        }

        if has_pdf_ok:
            new_text = _build_ad_text(sb, run_id, payload["slug"] or stock, payload, event="PRICE_CHANGED")
        elif DAILY_FIX_FALLBACK:
            new_text = generate_facebook_text(TEXT_ENGINE_URL, payload["slug"] or stock, "PRICE_CHANGED", payload)
        else:
            new_text = None

        if new_text:
            cur_text = (p.get("base_text") or "").strip()
            if cur_text.strip() != new_text.strip():
                if not DRY_RUN:
                    try:
                        update_post_text(post_id, FB_TOKEN, new_text)
                        sb.table("posts").update({
                            "base_text": new_text,
                            "last_updated_at": utc_now_iso(),
                            "status": "ACTIVE",
                        }).eq("post_id", post_id).execute()
                        time.sleep(max(2, DAILY_FIX_SLEEP))
                    except Exception as e:
                        log_event(sb, stock, "DAILY_TEXT_FIX_FAIL", {"post_id": post_id, "err": str(e), "run_id": run_id})
                text_fixed += 1

        checked += 1

    return {
        "checked": checked,
        "restored": restored,
        "text_fixed": text_fixed,
        "missing_post": missing_post,
        "site_count": len(site_stocks),
    }

def build_meta_vehicle_feed_csv(current: Dict[str, Any]) -> bytes:
    fieldnames = ["id","title","description","availability","condition","price","link","image_link","brand","year"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()

    for _, v in (current or {}).items():
        stock = (v.get("stock") or "").strip().upper()
        url = (v.get("url") or "").strip()
        title = (v.get("title") or "").strip()
        price_int = v.get("price_int")

        if not isinstance(price_int, int):
            digits = "".join(ch for ch in (v.get("price") or "") if ch.isdigit())
            price_int = int(digits) if digits else None

        photos = v.get("photos") or []
        image_link = (photos[0] or "").strip() if photos else ""
        if not image_link and ALLOW_NO_PHOTO and NO_PHOTO_URL:
            image_link = NO_PHOTO_URL

        if not stock or not url or not title or not isinstance(price_int, int):
            continue
        if not image_link:
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

def _site_price_quick(url: str) -> Optional[int]:
    try:
        r = SESSION.get(url, timeout=25)
        if not r.ok:
            return None
        txt = r.text or ""
    except Exception:
        return None
    m = re.search(r"(\d[\d\s]{2,})\s*\$", txt)
    if not m:
        return None
    digits = "".join(ch for ch in m.group(1) if ch.isdigit())
    return int(digits) if digits else None

def meta_vs_site_report(current: Dict[str, Any]) -> bytes:
    rows = []
    n = 0
    for _, v in current.items():
        if n >= COMPARE_META_LIMIT:
            break
        stock = (v.get("stock") or "").strip().upper()
        url = (v.get("url") or "").strip()
        p = v.get("price_int")
        if not stock or not url or not isinstance(p, int):
            continue
        site_p = _site_price_quick(url)
        status = "OK"
        if site_p is None:
            status = "SITE_PRICE_MISSING"
        elif site_p != p:
            status = "PRICE_MISMATCH"
        rows.append((stock, url, p, site_p, status))
        n += 1

    out = io.StringIO()
    out.write("stock,url,price_int,site_price_int,status\n")
    for stock, url, p, site_p, status in rows:
        out.write(f"{stock},{url},{p},{'' if site_p is None else site_p},{status}\n")
    return out.getvalue().encode("utf-8")

def refresh_no_photo_daily(sb, run_id: str, current: Dict[str, Any]) -> int:
    if RUN_MODE != "FULL" or not REFRESH_NO_PHOTO_DAILY:
        return 0
    if not (ALLOW_NO_PHOTO and NO_PHOTO_URL):
        return 0
    if not _try_daily_guard(sb, "no_photo_refresh"):
        return 0

    targets = []
    for slug, v in current.items():
        photos = v.get("photos") or []
        first = (photos[0] or "").strip() if photos else ""
        if not first or first == NO_PHOTO_URL:
            url = (v.get("url") or "").strip()
            if url:
                targets.append((slug, url))

    targets = targets[:max(0, REFRESH_NO_PHOTO_LIMIT)]
    if not targets:
        log_event(sb, "NO_PHOTO", "NO_PHOTO_REFRESH_NONE", {"run_id": run_id})
        return 0

    fixed = 0
    for slug, url in targets:
        try:
            fresh = parse_vehicle_detail_simple(SESSION, url)
            new_photos = fresh.get("photos") or []
            if not new_photos:
                continue
            first = (new_photos[0] or "").strip()
            if not first or first == NO_PHOTO_URL:
                continue

            current[slug]["photos"] = new_photos
            upsert_inventory(sb, [{
                "slug": slug,
                "stock": fresh.get("stock") or current[slug].get("stock"),
                "url": url,
                "title": fresh.get("title") or current[slug].get("title"),
                "vin": fresh.get("vin") or current[slug].get("vin"),
                "price_int": fresh.get("price_int") if fresh.get("price_int") is not None else current[slug].get("price_int"),
                "km_int": fresh.get("km_int") if fresh.get("km_int") is not None else current[slug].get("km_int"),
                "status": "ACTIVE",
                "updated_at": utc_now_iso(),
            }])

            fixed += 1
        except Exception:
            continue

    log_event(sb, "NO_PHOTO", "NO_PHOTO_REFRESH_DONE", {"run_id": run_id, "checked": len(targets), "fixed": fixed})
    return fixed

import re

CLEAN_WITHWITHOUT_DAILY = os.getenv("KENBOT_CLEAN_WITHWITHOUT_DAILY", "1").strip() == "1"
CLEAN_WITHWITHOUT_LIMIT = int(os.getenv("KENBOT_CLEAN_WITHWITHOUT_LIMIT", "5000").strip() or "5000")

_STOCK_FILE_RE = re.compile(r"^([0-9A-Z]+)_(facebook|marketplace)\.txt$", re.I)

def cleanup_with_without_daily(sb, run_id: str) -> dict:
    if RUN_MODE != "FULL" or not CLEAN_WITHWITHOUT_DAILY:
        return {"skipped": "disabled_or_not_full"}
    if not _try_daily_guard(sb, "clean_withwithout"):
        return {"skipped": "already_done_today"}

    # stocks actifs
    inv = (sb.table("inventory")
             .select("stock")
             .eq("status", "ACTIVE")
             .limit(5000)
             .execute().data) or []
    active = set((r.get("stock") or "").strip().upper() for r in inv if (r.get("stock") or "").strip())

    deleted = 0
    checked = 0

    for prefix in ["with", "without"]:
        try:
            items = sb.storage.from_(OUTPUTS_BUCKET).list(prefix) or []
        except Exception:
            continue

        to_del = []
        for it in items:
            name = it.get("name") or ""
            if "." not in name:
                continue  # ignore folders like assets
            m = _STOCK_FILE_RE.match(name)
            if not m:
                continue
            st = m.group(1).upper()
            checked += 1
            if st not in active:
                to_del.append(f"{prefix}/{name}")

        # safety cap
        to_del = to_del[:CLEAN_WITHWITHOUT_LIMIT]

        if to_del and not DRY_RUN:
            for i in range(0, len(to_del), 200):
                sb.storage.from_(OUTPUTS_BUCKET).remove(to_del[i:i+200])
            deleted += len(to_del)

    log_event(sb, "CLEAN", "WITHWITHOUT_CLEAN", {"run_id": run_id, "checked": checked, "deleted": deleted})
    return {"checked": checked, "deleted": deleted}

def main() -> None:
    sb = get_client(SUPABASE_URL, SUPABASE_KEY)
    acquire_lock_or_exit(sb)

    now = utc_now_iso()
    run_id = _run_id_from_now(now)

    try:
        upsert_scrape_run(sb, run_id, status="RUNNING", note=f"cron_prod mode={RUN_MODE}")

        cleanup_storage_runs(sb, RAW_BUCKET, "raw_pages", keep=RAW_KEEP)
        cleanup_storage_runs(sb, SNAP_BUCKET, "runs", keep=SNAP_KEEP)
        cleanup_storage_runs(sb, OUTPUTS_BUCKET, "runs", keep=OUTPUT_RUNS_KEEP)

        inv_db = get_inventory_map(sb)  # map par slug, mais values contiennent stock
        posts_db = get_posts_map(sb)

        inv_by_stock = _best_row_per_stock(list(inv_db.values()))

        listing_url = f"{BASE_URL}{INVENTORY_PATH}"
        page_urls = [
            listing_url,
            listing_url.rstrip("/") + "?page=2",
            listing_url.rstrip("/") + "?page=3",
        ]

        current: Dict[str, Dict[str, Any]] = {}
        raw_meta = {"listing_url": listing_url, "pages": len(page_urls), "ts": now}
        detail_urls: List[str] = []

        for idx, page_url in enumerate(page_urls, start=1):
            try:
                html_text = fetch_html(SESSION, page_url, timeout=35)
                html_bytes = (html_text or "").encode("utf-8", errors="ignore")
                storage_path = f"raw_pages/{run_id}/kennebec_page_{idx}.html"

                upload_bytes_to_storage(sb, RAW_BUCKET, storage_path, html_bytes,
                                       content_type="text/html; charset=utf-8", upsert=True)
                upsert_raw_page(sb, run_id, page_no=idx, storage_path=storage_path, data=html_bytes)

                detail_urls.extend(parse_inventory_listing_urls(BASE_URL, INVENTORY_PATH, html_text))
            except Exception as e:
                log_event(sb, "SCRAPE", "LISTING_PAGE_FAIL", {"page_url": page_url, "err": str(e), "run_id": run_id})

        detail_urls = list(dict.fromkeys(detail_urls))

        for u in detail_urls:
            try:
                v = parse_vehicle_detail_simple(SESSION, u)
                stock = (v.get("stock") or "").strip().upper()
                title = (v.get("title") or "").strip()
                if not stock or not title:
                    continue

                existing = inv_by_stock.get(stock) or {}
                stable_slug = (existing.get("slug") or "").strip()
                slug = stable_slug if stable_slug else slugify(title, stock)

                v["slug"] = slug
                current[slug] = v
            except Exception:
                continue

        inv_count = len(current)
        raw_meta["inventory_count"] = inv_count
        upload_json_to_storage(sb, RAW_BUCKET, f"raw_pages/{run_id}/meta.json", raw_meta, upsert=True)

        inv_db_active = {s: r for s, r in inv_db.items() if (r.get("status") or "").upper() in ("ACTIVE", "MISSING")}
        db_active_count = len(inv_db_active) or 1

        scrape_ok = True
        if inv_count < MIN_INVENTORY_ABS:
            scrape_ok = False
        if (inv_count / db_active_count) < MIN_INVENTORY_RATIO:
            scrape_ok = False

        if not scrape_ok:
            log_event(sb, "SCRAPE", "SCRAPE_TOO_SMALL", {
                "inv_count": inv_count,
                "db_active_count": db_active_count,
                "min_abs": MIN_INVENTORY_ABS,
                "min_ratio": MIN_INVENTORY_RATIO,
                "run_id": run_id,
            })

        if scrape_ok:
            refresh_no_photo_daily(sb, run_id, current)

        if RUN_MODE == "FULL" and CACHE_STICKERS:
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
                    if st == "ok":
                        ok += 1
                    elif st == "bad":
                        bad += 1
                    else:
                        skip += 1
                except Exception:
                    skip += 1
            log_event(sb, "STICKER", "STICKER_SUMMARY", {"ok": ok, "bad": bad, "skip": skip, "total": len(vins), "run_id": run_id})

        # Upsert inventory ACTIVE (avec stock)
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
        if rows:
            upsert_inventory(sb, rows)

        current_slugs = set(current.keys())
        db_slugs = set(inv_db_active.keys())

        disappeared_slugs = sorted(db_slugs - current_slugs)
        new_slugs = sorted(current_slugs - db_slugs)
        common_slugs = sorted(current_slugs & db_slugs)

        if scrape_ok:
            daily_stats = daily_audit_and_fix(sb, run_id)
            log_event(sb, "DAILY", "DAILY_FIX", {"run_id": run_id, **daily_stats})

            ww = cleanup_with_without_daily(sb, run_id)
            log_event(sb, "CLEAN", "WITHWITHOUT_CLEAN_RESULT", {"run_id": run_id, **ww})

        # Recovered MISSING -> ACTIVE (inclut stock si possible)
        for slug in common_slugs:
            old = inv_db.get(slug) or {}
            if (old.get("status") or "").upper() == "MISSING":
                st = (old.get("stock") or "").strip().upper()
                payload = {"slug": slug, "status": "ACTIVE", "updated_at": now, "last_seen": now}
                if st:
                    payload["stock"] = st
                upsert_inventory(sb, [payload])
                log_event(sb, slug, "RECOVERED_ACTIVE", {"run_id": run_id})

        # SOLD flow fiable
        if scrape_ok:
            for slug in disappeared_slugs:
                old_inv = inv_db.get(slug) or {}
                old_status = (old_inv.get("status") or "").upper()
                st = (old_inv.get("stock") or "").strip().upper()

                post = posts_db.get(slug) or {}
                post_id = post.get("post_id")

                if old_status == "ACTIVE":
                    payload = {"slug": slug, "status": "MISSING", "updated_at": now}
                    if st:
                        payload["stock"] = st
                    upsert_inventory(sb, [payload])
                    log_event(sb, slug, "MISSING_1", {"post_id": post_id, "run_id": run_id})
                    continue

                if old_status == "MISSING":
                    if post_id and str(post.get("status", "")).upper() != "SOLD":
                        if DRY_RUN:
                            print(f"DRY_RUN: would MARK SOLD -> {slug} (post_id={post_id})", flush=True)
                        else:
                            try:
                                base_text = (post.get("base_text") or "").strip()
                                if not base_text:
                                    from fb_api import fetch_fb_post_message
                                    base_text = _strip_sold_banner(fetch_fb_post_message(post_id, FB_TOKEN))
                                msg = _make_sold_message(base_text)
                                update_post_text(post_id, FB_TOKEN, msg)

                                upsert_post(sb, {
                                    "slug": slug,
                                    "post_id": post_id,
                                    "status": "SOLD",
                                    "sold_at": now,
                                    "last_updated_at": now,
                                    "base_text": base_text,
                                    "stock": post.get("stock") or st,
                                })
                                log_event(sb, slug, "SOLD_CONFIRMED", {"post_id": post_id, "run_id": run_id})
                            except Exception as e:
                                log_event(sb, slug, "FB_SOLD_FAIL", {"post_id": post_id, "err": str(e), "run_id": run_id})

                    payload = {"slug": slug, "status": "SOLD", "updated_at": now}
                    if st:
                        payload["stock"] = st
                    upsert_inventory(sb, [payload])
        else:
            if disappeared_slugs:
                log_event(sb, "SCRAPE", "SKIP_SOLD_DUE_TO_BAD_SCRAPE", {"count": len(disappeared_slugs), "run_id": run_id})

        # PRICE_CHANGED
        if scrape_ok:
            for slug in common_slugs:
                old = inv_db.get(slug) or {}
                new = current.get(slug) or {}
                if old.get("price_int") is None or new.get("price_int") is None:
                    continue
                if old.get("price_int") == new.get("price_int"):
                    continue

                post = posts_db.get(slug) or {}
                post_id = post.get("post_id")
                if not post_id:
                    continue
                if (post.get("status") or "").upper() == "SOLD":
                    continue

                if DRY_RUN:
                    print(f"DRY_RUN: would UPDATE PRICE text -> {slug} ({old.get('price_int')} -> {new.get('price_int')})", flush=True)
                    continue

                try:
                    msg = _build_ad_text(sb, run_id, slug, new, event="PRICE_CHANGED")
                    update_post_text(post_id, FB_TOKEN, msg)
                    upsert_post(sb, {
                        "slug": slug,
                        "post_id": post_id,
                        "status": "ACTIVE",
                        "last_updated_at": now,
                        "base_text": _strip_sold_banner(msg),
                        "stock": new.get("stock"),
                    })
                    log_event(sb, slug, "PRICE_CHANGED_UPDATED", {"post_id": post_id, "run_id": run_id})
                except Exception as e:
                    log_event(sb, slug, "FB_PRICE_UPDATE_FAIL", {"post_id": post_id, "err": str(e), "run_id": run_id})

        # NEW posts
        posted = 0
        for slug in new_slugs[:max(0, MAX_TARGETS)]:
            v = current.get(slug) or {}
            stock = (v.get("stock") or "").strip().upper()
            photos = v.get("photos") or []

            if DRY_RUN:
                print(f"DRY_RUN: would POST NEW -> {slug} ({stock})", flush=True)
                posted += 1
                continue

            try:
                msg = _build_ad_text(sb, run_id, slug, v, event="NEW")

                photo_paths = _download_photos(stock, photos, limit=MAX_PHOTOS)
                if not photo_paths:
                    log_event(sb, slug, "NEW_SKIP_NO_PHOTOS", {"run_id": run_id})
                    continue

                media_ids = publish_photos_unpublished(FB_PAGE_ID, FB_TOKEN, photo_paths[:POST_PHOTOS], limit=POST_PHOTOS)
                post_id = create_post_with_attached_media(FB_PAGE_ID, FB_TOKEN, msg, media_ids)

                extra = photo_paths[POST_PHOTOS:]
                if extra:
                    try:
                        publish_photos_as_comment_batch(FB_PAGE_ID, FB_TOKEN, post_id, extra)
                    except Exception:
                        pass

                upsert_post(sb, {
                    "slug": slug,
                    "post_id": post_id,
                    "status": "ACTIVE",
                    "created_at": now,
                    "last_updated_at": now,
                    "base_text": _strip_sold_banner(msg),
                    "stock": stock,
                })
                log_event(sb, slug, "NEW_POSTED", {"post_id": post_id, "stock": stock, "run_id": run_id})

                posted += 1
                time.sleep(max(1, SLEEP_BETWEEN))
            except Exception as e:
                log_event(sb, slug, "FB_NEW_FAIL", {"err": str(e), "run_id": run_id})

        # Meta feed + report (FULL only)
        if RUN_MODE == "FULL" and BUILD_META_FEEDS:
            feed_bytes = build_meta_vehicle_feed_csv(current)
            upload_bytes_to_storage(sb, OUTPUTS_BUCKET, "feeds/meta_vehicle.csv", feed_bytes,
                                   content_type="text/csv; charset=utf-8", upsert=True)
            upload_bytes_to_storage(sb, OUTPUTS_BUCKET, f"runs/{run_id}/feeds/meta_vehicle.csv", feed_bytes,
                                   content_type="text/csv; charset=utf-8", upsert=True)
            log_event(sb, "META", "META_FEED_UPLOADED", {"run_id": run_id, "rows": len(current)})

        if RUN_MODE == "FULL" and COMPARE_META_VS_SITE:
            report_bytes = meta_vs_site_report(current)
            upload_bytes_to_storage(sb, OUTPUTS_BUCKET, "reports/meta_vs_site.csv", report_bytes,
                                   content_type="text/csv; charset=utf-8", upsert=True)
            upload_bytes_to_storage(sb, OUTPUTS_BUCKET, f"runs/{run_id}/reports/meta_vs_site.csv", report_bytes,
                                   content_type="text/csv; charset=utf-8", upsert=True)

        upsert_scrape_run(sb, run_id, status="OK", note=f"cron_prod mode={RUN_MODE} posted={posted} inv={inv_count}")
        print(f"✅ cron_prod done run_id={run_id} mode={RUN_MODE} inv={inv_count} new_posted={posted} scrape_ok={scrape_ok}", flush=True)

    finally:
        release_lock(sb)


if __name__ == "__main__":
    main()

