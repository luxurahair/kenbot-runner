from supabase import create_client, Client
from typing import Any, Dict, List
from datetime import datetime, timezone

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_client(url: str, key: str) -> Client:
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY manquants.")
    return create_client(url, key)

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
