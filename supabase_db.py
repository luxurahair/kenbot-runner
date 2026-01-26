import os
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client, Client
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import json
import hashlib


# =========================
# Time
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# Storage helpers
# =========================
def upload_json_to_storage(sb: Client, bucket: str, path: str, obj: Any, upsert: bool = True) -> None:
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    opts = {"content-type": "application/json"}
    if upsert:
        opts["upsert"] = "true"
    sb.storage.from_(bucket).upload(path, data, file_options=opts)


def read_json_from_storage(sb: Client, bucket: str, path: str) -> dict:
    try:
        res = sb.storage.from_(bucket).download(path)
    except Exception:
        return {}
    if not res:
        return {}
    try:
        return json.loads(res.decode("utf-8"))
    except Exception:
        return {}


def upload_bytes_to_storage(
    sb: Client,
    bucket: str,
    path: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    upsert: bool = True,
) -> None:
    opts = {"content-type": content_type}
    if upsert:
        opts["upsert"] = "true"
    sb.storage.from_(bucket).upload(path, data, file_options=opts)


def list_storage(sb: Client, bucket: str, folder: str) -> List[Dict[str, Any]]:
    # Supabase list: folder sans leading slash
    return sb.storage.from_(bucket).list(folder) or []


def get_latest_snapshot_run_id(sb: Client, bucket: str, runs_folder: str = "runs") -> Optional[str]:
    items = list_storage(sb, bucket, runs_folder)
    names = sorted([it.get("name") for it in items if it.get("name")], reverse=True)
    return names[0] if names else None


def cleanup_storage_runs(sb: Client, bucket: str, folder: str, keep: int = 2) -> int:
    """
    folder ex: 'raw_pages' ou 'runs'
    Garde les keep plus récents, supprime le reste.
    Retourne le nombre de runs supprimés.
    """
    if keep < 0:
        keep = 0

    items = list_storage(sb, bucket, folder)
    names = sorted([it.get("name") for it in items if it.get("name")], reverse=True)
    to_delete = names[keep:]

    deleted = 0
    for run_id in to_delete:
        if not run_id:
            continue
        files = list_storage(sb, bucket, f"{folder}/{run_id}")
        paths = []
        for f in files:
            fn = f.get("name")
            if fn:
                paths.append(f"{folder}/{run_id}/{fn}")
        if paths:
            sb.storage.from_(bucket).remove(paths)
        deleted += 1

    return deleted


# =========================
# Client
# =========================
from supabase import create_client, Client

def get_client(url: str | None = None, key: str | None = None) -> Client:
    # fallback env si non fourni
    url = (url or os.getenv("SUPABASE_URL") or "").strip()
    key = (key or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    base = url.rstrip("/")  # base sans slash
    sb = create_client(base, key)

    # Force endpoint storage avec slash (évite warnings)
    try:
        sb.storage_url = f"{base}/storage/v1/"
    except Exception:
        pass

    return sb


# =========================
# Core tables: inventory / posts / events
# =========================
def upsert_inventory(sb: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sb.table("inventory").upsert(rows, on_conflict="slug").execute()


def get_inventory_map(sb: Client) -> Dict[str, Dict[str, Any]]:
    res = sb.table("inventory").select("*").execute()
    data = res.data or []
    return {r["slug"]: r for r in data if r.get("slug")}


def upsert_post(sb: Client, row: Dict[str, Any]) -> None:
    sb.table("posts").upsert(row, on_conflict="slug").execute()


def get_posts_map(sb: Client) -> Dict[str, Dict[str, Any]]:
    res = sb.table("posts").select("*").execute()
    data = res.data or []
    return {r["slug"]: r for r in data if r.get("slug")}


def log_event(sb: Client, slug: str, typ: str, payload: Dict[str, Any]) -> None:
    sb.table("events").insert({"slug": slug, "type": typ, "payload": payload}).execute()


# =========================
# Mémoire tables (ALIGNED to your schema)
# =========================
def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data or b"").hexdigest()


def upsert_scrape_run(sb: Client, run_id: str, status: str = "OK", note: str = "") -> None:
    """
    scrape_runs:
      run_id (text, NOT NULL)
      created_at (timestamptz, NOT NULL)
      status (text, NOT NULL)
      note (text, nullable)
    """
    sb.table("scrape_runs").upsert(
        {
            "run_id": run_id,
            "created_at": utc_now_iso(),
            "status": status,
            "note": (note or None),
        },
        on_conflict="run_id",
    ).execute()


def upsert_raw_page(sb: Client, run_id: str, page_no: int, storage_path: str, data: bytes) -> None:
    """
    raw_pages:
      run_id (text, NOT NULL)
      page_no (int4, NOT NULL)
      storage_path (text, NOT NULL)
      bytes (int4, NOT NULL)
      sha256 (text, NOT NULL)
    """
    sb.table("raw_pages").upsert(
        {
            "run_id": run_id,
            "page_no": int(page_no),
            "storage_path": storage_path,
            "bytes": len(data or b""),
            "sha256": sha256_hex(data or b""),
        },
        on_conflict="run_id,page_no",
    ).execute()


def upsert_sticker_pdf(
    sb: Client,
    vin: str,
    status: str,
    storage_path: str,
    data: bytes,
    reason: str = "",
    run_id: str = "",
) -> None:
    """
    sticker_pdfs:
      vin (text, NOT NULL)
      status (text, NOT NULL)
      storage_path (text, NOT NULL)
      bytes (int4, NOT NULL)
      sha256 (text, NOT NULL)
      reason (text, nullable)
      run_id (text, nullable)
      updated_at (timestamptz, NOT NULL)
    """
    sb.table("sticker_pdfs").upsert(
        {
            "vin": (vin or "").strip().upper(),
            "status": status,
            "storage_path": storage_path,
            "bytes": len(data or b""),
            "sha256": sha256_hex(data or b""),
            "reason": (reason or None),
            "run_id": (run_id or None),
            "updated_at": utc_now_iso(),
        },
        on_conflict="vin",
    ).execute()


def upsert_output(
    sb: Client,
    stock: str,
    kind: str,
    facebook_path: str,
    marketplace_path: str,
    run_id: str = "",
) -> None:
    sb.table("outputs").upsert(
        {
            "stock": (stock or "").strip().upper(),
            "kind": (kind or "").strip(),
            "facebook_path": facebook_path,
            "marketplace_path": marketplace_path,
            "run_id": run_id or None,
            "updated_at": utc_now_iso(),
        },
        on_conflict="stock,kind",
    ).execute()

