#!/usr/bin/env python3
import os, csv, re
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from supabase_db import get_client, upload_json_to_storage, upload_bytes_to_storage, utc_now_iso

TIMEOUT = 20
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (KenBot meta-vs-site supabase)"})

REPORTS_BUCKET = os.getenv("SB_BUCKET_OUTPUTS", "kennebec-outputs").strip()
META_TABLE = "meta_feed_items"

def norm_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip().strip('"').strip("'").rstrip(",")
    p = urlsplit(u)
    return urlunsplit((p.scheme, p.netloc, p.path.rstrip("/"), "", ""))

def norm_money(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("$", "").replace(" ", "").replace(",", "")
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None

def fetch_site_price(url: str) -> Optional[int]:
    url = norm_url(url)
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    html = r.text

    m = re.search(r"displayedPrice\s*[:=]\s*['\"]([0-9]+(?:\.[0-9]+)?)['\"]", html, re.IGNORECASE)
    if m:
        try:
            return int(float(m.group(1)))
        except Exception:
            pass

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    for txt in soup.find_all(string=re.compile(r"\$", re.IGNORECASE)):
        t = re.sub(r"\s+", " ", str(txt)).strip()
        mm = re.search(r"(\d[\d\s.,]{2,})\s*\$", t)
        if mm:
            return norm_money(mm.group(1))
    return None

def read_meta_feed_csv(path: str) -> Dict[str, Dict[str, Any]]:
    """
    CSV Meta: colonnes typiques: url/link, sale_price/price, vehicle_id/id
    Indexé par url normalisée.
    """
    data: Dict[str, Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            url = norm_url(row.get("url") or row.get("link") or "")
            if not url:
                continue
            price = norm_money(row.get("sale_price") or row.get("price"))
            if price is None:
                continue
            data[url] = {
                "meta_price_int": price,
                "vehicle_id": (row.get("vehicle_id") or row.get("id") or "").strip(),
            }
    return data

def main():
    supa_url = os.getenv("SUPABASE_URL")
    supa_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supa_url or not supa_key:
        raise SystemExit("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    feed_path = os.getenv("META_FEED_CSV", "").strip()
    if not feed_path:
        raise SystemExit("Set META_FEED_CSV=/path/to/meta_used_vehicles.csv")

    sb = get_client(supa_url, supa_key)
    now = utc_now_iso()
    run_id = re.sub(r"[^0-9]", "", now)[:14]

    # 1) Load inventory from Supabase
    inv_rows = sb.table("inventory").select("slug,stock,url,price_int,status,vin").execute().data or []
    inv = []
    for r in inv_rows:
        if (r.get("status") or "").upper() != "ACTIVE":
            continue
        url = norm_url(r.get("url") or "")
        stock = (r.get("stock") or "").strip().upper()
        if not url or not stock:
            continue
        inv.append({"stock": stock, "url": url, "site_price_int": r.get("price_int")})

    # 2) Load Meta feed CSV
    meta = read_meta_feed_csv(feed_path)

    # 3) Upsert Meta base table
    up_rows = []
    for url, m in meta.items():
        up_rows.append({
            "url": url,
            "stock": None,
            "vehicle_id": m.get("vehicle_id") or None,
            "meta_price_int": m.get("meta_price_int"),
            "last_seen": now,
            "updated_at": now,
        })
    if up_rows:
        sb.table(META_TABLE).upsert(up_rows, on_conflict="url").execute()

    # 4) Compare
    report = []
    for it in inv:
        url = it["url"]
        stock = it["stock"]
        site_price = it["site_price_int"]

        meta_row = meta.get(url)
        if not meta_row:
            report.append({
                "stock": stock,
                "url": url,
                "site_price": site_price,
                "meta_price": None,
                "diff": None,
                "action": "META_MISSING",
                "vehicle_id_meta": None,
            })
            continue

        meta_price = meta_row["meta_price_int"]
        diff = (site_price - meta_price) if (site_price is not None and meta_price is not None) else None
        action = "OK" if diff == 0 else "PRICE_MISMATCH"

        report.append({
            "stock": stock,
            "url": url,
            "site_price": site_price,
            "meta_price": meta_price,
            "diff": diff,
            "action": action,
            "vehicle_id_meta": meta_row.get("vehicle_id") or None,
        })

    # 5) Save report in Supabase Storage (JSON + CSV)
    upload_json_to_storage(sb, REPORTS_BUCKET, f"reports/{run_id}/meta_vs_site.json", report, upsert=True)

    csv_lines = ["stock,url,site_price,meta_price,diff,action,vehicle_id_meta"]
    for r in report:
        csv_lines.append(
            f"{r['stock']},{r['url']},{r['site_price'] or ''},{r['meta_price'] or ''},{r['diff'] if r['diff'] is not None else ''},{r['action']},{r['vehicle_id_meta'] or ''}"
        )
    csv_bytes = ("\n".join(csv_lines) + "\n").encode("utf-8")
    upload_bytes_to_storage(sb, REPORTS_BUCKET, f"reports/{run_id}/meta_vs_site.csv", csv_bytes, content_type="text/csv; charset=utf-8", upsert=True)

    print(f"✅ meta_vs_site report uploaded -> {REPORTS_BUCKET}/reports/{run_id}/meta_vs_site.csv")
    print("Counts:", {
        "inventory_active": len(inv),
        "meta_rows": len(meta),
        "mismatch": sum(1 for r in report if r["action"] == "PRICE_MISMATCH"),
        "missing": sum(1 for r in report if r["action"] == "META_MISSING"),
    })

if __name__ == "__main__":
    main()
