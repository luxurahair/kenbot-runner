import json
import requests
from pathlib import Path
from typing import Dict, List

GRAPH_VER = "v24.0"

def _graph(url: str) -> str:
    return f"https://graph.facebook.com/{GRAPH_VER}/{url.lstrip('/')}"

def publish_photos_unpublished(page_id: str, token: str, photo_paths: List[Path], limit: int = 10) -> List[str]:
    media_ids: List[str] = []
    for p in photo_paths[:limit]:
        url = _graph(f"{page_id}/photos")
        with open(p, "rb") as f:
            resp = requests.post(
                url,
                params={"access_token": token},
                data={"published": "false"},
                files={"source": f},
                timeout=120,
            )
        j = resp.json()
        if not resp.ok:
            raise RuntimeError(f"FB upload photo error: {j}")
        mid = j.get("id")
        if not mid:
            raise RuntimeError(f"FB upload photo missing id: {j}")
        media_ids.append(mid)
    return media_ids

def create_post_with_attached_media(page_id: str, token: str, message: str, media_ids: List[str]) -> str:
    url = _graph(f"{page_id}/feed")
    data: Dict[str, str] = {"message": message}
    for i, mid in enumerate(media_ids):
        data[f"attached_media[{i}]"] = json.dumps({"media_fbid": mid})

    resp = requests.post(url, params={"access_token": token}, data=data, timeout=120)
    j = resp.json()
    if not resp.ok:
        raise RuntimeError(f"FB create post error: {j}")
    post_id = j.get("id")
    if not post_id:
        raise RuntimeError(f"FB create post missing id: {j}")
    return post_id

def update_post_text(post_id: str, token: str, message: str) -> None:
    url = _graph(post_id)
    resp = requests.post(url, params={"access_token": token}, data={"message": message}, timeout=60)
    j = resp.json()
    if not resp.ok:
        raise RuntimeError(f"FB update text error: {j}")

def comment_on_post(post_id: str, token: str, message: str) -> str:
    url = _graph(f"{post_id}/comments")
    resp = requests.post(url, params={"access_token": token}, data={"message": message}, timeout=60)
    j = resp.json()
    if not resp.ok:
        raise RuntimeError(f"FB comment error: {j}")
    return j.get("id", "")

def publish_photos_as_comment_batch(page_id: str, token: str, post_id: str, photo_paths: List[Path]) -> None:
    """
    Best-effort: commentaire "suite photos", puis publish photos (posts photo).
    """
    if not photo_paths:
        return

    try:
        comment_on_post(post_id, token, "📸 Suite des photos 👇")
    except Exception:
        pass

    for p in photo_paths:
        url = _graph(f"{page_id}/photos")
        with open(p, "rb") as f:
            resp = requests.post(
                url,
                params={"access_token": token},
                data={"caption": ""},
                files={"source": f},
                timeout=120,
            )
        j = resp.json()
        if not resp.ok:
            raise RuntimeError(f"FB publish extra photo error: {j}")
