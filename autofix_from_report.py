# autofix_from_report.py
import os, csv, time
from dotenv import load_dotenv
load_dotenv()

from supabase_db import get_client, get_posts_map, upsert_post, log_event
from fb_api import update_post_text
from runner import _make_sold_message  # si import possible; sinon on recopie la fonction

OUTPUTS_BUCKET = os.getenv("SB_BUCKET_OUTPUTS", "kennebec-outputs").strip()
REPORT_PATH = os.getenv("KENBOT_META_REPORT_PATH", "reports/meta_vs_site.csv").strip()

MAX_FIX = int(os.getenv("KENBOT_MAX_FIX", "6") or "6")
SLEEP = int(os.getenv("KENBOT_SLEEP_BETWEEN_POSTS", "60") or "60")
FB_TOKEN = os.getenv("KENBOT_FB_ACCESS_TOKEN") or ""
DRY = os.getenv("KENBOT_DRY_RUN", "0").strip() == "1"

def download_report(sb):
    return sb.storage.from_(OUTPUTS_BUCKET).download(REPORT_PATH)

def parse_rows(b: bytes):
    txt = b.decode("utf-8", errors="replace")
    r = csv.DictReader(txt.splitlines())
    return list(r)

def main():
    sb = get_client()
    posts_db = get_posts_map(sb)

    b = download_report(sb)
    rows = parse_rows(b)

    actions = []
    for row in rows:
        stock = (row.get("stock") or "").strip().upper()
        status = (row.get("status") or "").strip().upper()
        if not stock or not status:
            continue
        if status in ("MISSING_ON_SITE", "SOLD_ON_SITE"):
            actions.append(("SOLD", stock, row))
        elif status in ("PRICE_MISMATCH", "PRICE_CHANGED"):
            actions.append(("PRICE", stock, row))

    if not actions:
        print("No actions from report.")
        return

    done = 0
    for kind, stock, row in actions:
        if done >= MAX_FIX:
            break

        # retrouver le slug via posts_db (posts_map est par slug, pas par stock)
        # on cherche une entrée dont stock match
        target_slug = None
        target_post_id = None
        for slug, info in (posts_db or {}).items():
            if (info or {}).get("stock", "").strip().upper() == stock:
                target_slug = slug
                target_post_id = (info or {}).get("post_id")
                break

        if not target_slug or not target_post_id:
            continue

        if kind == "SOLD":
            new_text = _make_sold_message((info or {}).get("base_text") or "")
            if DRY:
                print(f"DRY SOLD {stock} {target_post_id}")
            else:
                update_post_text(target_post_id, FB_TOKEN, new_text)
                upsert_post(sb, {"slug": target_slug, "post_id": target_post_id, "status": "SOLD"})
            log_event(sb, target_slug, "AUTO_SOLD", {"stock": stock, "post_id": target_post_id})
            done += 1
            time.sleep(SLEEP)

        elif kind == "PRICE":
            # ici: soit tu reconstruis texte depuis runner outputs, soit tu modifies la ligne prix dans base_text
            # solution simple: laisse runner régénérer via text-engine => on déclenche un event "FORCE_UPDATE_PRICE"
            log_event(sb, target_slug, "AUTO_PRICE_MISMATCH", {"stock": stock, "post_id": target_post_id, "row": row})
            done += 1
            time.sleep(SLEEP)

    print(f"Done actions={done}")

if __name__ == "__main__":
    main()
