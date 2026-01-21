from supabase import create_client, Client
from typing import Any, Dict, List
from datetime import datetime, timezone

import json
from typing import Optional

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
    upsert: bool = True
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
    # items: [{"name": "20260120_143012", ...}, ...]
    names = sorted([it.get("name") for it in items if it.get("name")], reverse=True)
    return names[0] if names else None

def get_client(url: str, key: str) -> Client:
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY manquants.")

    base = url.strip().rstrip("/")  # base sans slash

    try:
        from supabase.lib.client_options import ClientOptions
        opts = ClientOptions(storage_url=f"{base}/storage/v1/")
        return create_client(base, key, options=opts)
    except Exception:
        return create_client(base, key)

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
