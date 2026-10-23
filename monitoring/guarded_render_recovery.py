"""Bounded Render API recovery; opt-in, single target, fail closed.

No Git, Supabase, backup, or Mac write operations.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from monitoring.safe_monitor import probe

SERVICE_ID = "srv-d7da02tckfvc73en2g9g"
SERVICE_NAME = "kenbot-dashboard-api"
HEALTH_URL = "https://kenbot-dashboard-api.onrender.com/api/health"
API_ROOT = "https://api.render.com/v1"
MAX_RESTARTS_PER_DAY = 2

def can_restart(deploys, now):
    """Use all recent deploys as a conservative persistent attempt budget."""
    if not isinstance(deploys, list) or not deploys:
        return False, "Deploy history unavailable"
    recent = []
    try:
        for item in deploys:
            entry = item.get("deploy", item)
            date = datetime.fromisoformat(entry["createdAt"].replace("Z","+00:00"))
            if date.tzinfo is None:
                return False, "Naive deploy timestamp"
            if date > now + timedelta(minutes=5):
                return False, "Invalid deploy timestamp"
            if now - timedelta(hours=24) <= date <= now:
                recent.append((date, entry["status"]))
    except (KeyError, ValueError, TypeError, AttributeError):
        return False, "Invalid deploy history"
    if any(status not in ("live", "deactivated", "failed", "canceled") for _, status in recent):
        return False, "Deployment already in progress"
    if len(recent) >= MAX_RESTARTS_PER_DAY:
        return False, "Deployment budget exceeded"
    if recent and now - max(t for t, _ in recent) < timedelta(hours=1):
        return False, "One-hour cooldown"
    return True, "Eligible"

def api_request(method, path, token, payload=None):
    if not token:
        raise ValueError("Missing Render API token")
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        API_ROOT + path, data=data, method=method,
        headers={"Authorization": "Bearer " + token, "Accept": "application/json",
                 "Content-Type": "application/json", "User-Agent": "kenbot-safe-recovery/2"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        body = response.read(1024 * 1024)
        return json.loads(body) if body else {}

def eligible_service(service):
    if not isinstance(service, dict):
        return False
    return (service.get("id") == SERVICE_ID
            and service.get("name") == SERVICE_NAME
            and service.get("type") == "web_service"
            and service.get("repo", "").rstrip("/") == "https://github.com/luxurahair/kenebec-ai"
            and service.get("suspended") == "not_suspended")

def run(*, enabled=False, token="", api=api_request, check=probe, sleeper=time.sleep, now=None):
    """A single guarded attempt. Return (status, message); never retry restart."""
    now = now or datetime.now(timezone.utc)
    first = check(HEALTH_URL)
    if first["status"] == "HEALTHY":
        return "HEALTHY", "Service healthy"
    if first["status"] not in ("SERVICE_ERROR", "NETWORK_ERROR"):
        return "INVESTIGATE", "Not a restartable fault"
    sleeper(65)
    second = check(HEALTH_URL)
    if second["status"] == "HEALTHY":
        return "RECOVERED", "Transient failure resolved"
    if second["status"] not in ("SERVICE_ERROR", "NETWORK_ERROR"):
        return "INVESTIGATE", "Unconfirmed fault"
    if not enabled:
        return "DISABLED", "Confirmed outage; restart not enabled"
    if not token:
        return "BLOCKED", "Missing Render API token"
    try:
        service = api("GET", "/services/" + SERVICE_ID, token)
        if not eligible_service(service):
            return "BLOCKED", "Service identity mismatch"
        history = api("GET", "/services/" + SERVICE_ID + "/deploys?limit=20", token)
        ok, reason = can_restart(history, now)
        if not ok:
            return "BLOCKED", reason
        # Recheck just before mutation. No write unless still failing.
        third = check(HEALTH_URL)
        if third["status"] not in ("SERVICE_ERROR", "NETWORK_ERROR"):
            return "RECOVERED", "Recovered before restart"
        api("POST", "/services/" + SERVICE_ID + "/restart", token)
        return "RESTART_REQUESTED", "One bounded restart requested; post-restart check required"
    except (ValueError, KeyError, TypeError, urllib.error.URLError, TimeoutError, OSError):
        return "BLOCKED", "Render API unavailable or invalid response"

def main():
    status, message = run(
        enabled=os.getenv("KENBOT_AUTOREPAIR_ENABLED") == "1",
        token=os.getenv("RENDER_API_KEY", ""),
    )
    print(json.dumps({"status":status, "message":message}))
    # Never mark a confirmed unresolved outage healthy.
    return 0 if status in ("HEALTHY","RECOVERED","RESTART_REQUESTED") else 1

if __name__ == "__main__":
    sys.exit(main())
