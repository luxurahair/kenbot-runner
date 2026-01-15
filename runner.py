import os
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

DRY_RUN = os.getenv("KENBOT_DRY_RUN", "0").strip() == "1"

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

TMP_PHOTOS = Path(os.getenv("KENBOT_TMP_PHOTOS_DIR", "/tmp/kenbot_photos"))
TMP_PHOTOS.mkdir(parents=True, exist_ok=True)

MAX_PHOTOS = 15
POST_PHOTOS = 10  # 10 dans le post, 5 extra best-effort

def download_photo(url: str, dest: Path) -> None:
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)

def download_photos(slug: str, urls: List[str], limit: int) -> List[Path]:
    out: List[Path] = []
    folder = TMP_PHOTOS / slug
    folder.mkdir(parents=True, exist_ok=True)

    for i, u in enumerate(urls[:limit], start=1):
        ext = ".jpg"
        low = u.lower()
        if ".png" in low:
            ext = ".png"
        elif ".webp" in low:
            ext = ".webp"
        p = folder / f"{slug}_{i:02d}{ext}"
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

def main() -> None:
    sb = get_client(SUPABASE_URL, SUPABASE_KEY)

    inv_db = get_inventory_map(sb)
    posts_db = get_posts_map(sb)

    # 1) Fetch 3 pages listing
    urls: List[str] = []
    pages = [
        f"{BASE_URL}{INVENTORY_PATH}",
        f"{BASE_URL}{INVENTORY_PATH}?page=2",
        f"{BASE_URL}{INVENTORY_PATH}?page=3",
    ]
    for page in pages:
        html = SESSION.get(page, timeout=30).text
        urls += parse_inventory_listing_urls(BASE_URL, INVENTORY_PATH, html)

    urls = sorted(list(dict.fromkeys(urls)))

    # 2) Build current inventory map
    current: Dict[str, Dict[str, Any]] = {}
    for url in urls:
        d = parse_vehicle_detail_simple(SESSION, url)
        stock = (d.get("stock") or "").strip().upper()
        title = (d.get("title") or "").strip()
        if not stock or not title:
            continue
        slug = slugify(title, stock)
        d["slug"] = slug
        current[slug] = d

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
                except Exception:
                    pass

        upsert_post(sb, {
            "slug": slug,
            "post_id": post_id,
            "status": "SOLD",
            "sold_at": now,
            "last_updated_at": now,
        })
        log_event(sb, slug, "SOLD", {"slug": slug})

    # 5) PRICE_CHANGED
    price_changed: List[str] = []
    for slug in common_slugs:
        old = inv_db.get(slug) or {}
        new = current.get(slug) or {}
        if (old.get("price_int") is not None) and (new.get("price_int") is not None) and old.get("price_int") != new.get("price_int"):
            price_changed.append(slug)

    # 6) NEW + PRICE_CHANGED -> generate via text-engine
    targets: List[Tuple[str, str]] = [(s, "NEW") for s in new_slugs] + [(s, "PRICE_CHANGED") for s in price_changed]

    for slug, event in targets:
        v = current[slug]
        vehicle_payload = {
            "title": v.get("title") or "",
            "price": f"{v.get('price_int'):,}".replace(",", " ") + " $" if v.get("price_int") else (v.get("price") or ""),
            "mileage": f"{v.get('km_int'):,}".replace(",", " ") + " km" if v.get("km_int") else (v.get("mileage") or ""),
            "stock": (v.get("stock") or "").strip().upper(),
            "vin": (v.get("vin") or "").strip().upper(),
            "url": v.get("url") or "",
        }

        fb_text = generate_facebook_text(TEXT_ENGINE_URL, slug=slug, event=event, vehicle=vehicle_payload)

        post_info = posts_db.get(slug) or {}
        post_id = post_info.get("post_id")

        photo_urls = v.get("photos") or []
        photo_paths = download_photos(slug, photo_urls, limit=MAX_PHOTOS)
       
        if DRY_RUN:
            if not post_id:
                print(f"DRY_RUN: would PUBLISH NEW -> {slug} (photos={len(photo_paths)})")
                log_event(sb, slug, "NEW", {"dry_run": True, "photos": len(photo_paths)})
            else:
                print(f"DRY_RUN: would UPDATE PRICE_CHANGED -> {slug} (post_id={post_id})")
                log_event(sb, slug, "PRICE_CHANGED", {"dry_run": True, "post_id": post_id})
            continue
       
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
            log_event(sb, slug, "NEW", {"post_id": post_id, "photos": len(photo_paths)})

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
            except Exception:
                pass

    print(f"OK: NEW={len(new_slugs)} SOLD={len(disappeared_slugs)} PRICE_CHANGED={len(price_changed)}")

if __name__ == "__main__":
    main()
