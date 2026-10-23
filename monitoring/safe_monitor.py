"""Bounded, read-only health monitoring shared by GitHub Actions and unit tests."""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.error
import urllib.request

DEFAULT_SERVICES = {"kenbot-api": "https://kenbot-dashboard-api.onrender.com/api/health"}
MAX_ATTEMPTS = 3
TIMEOUT_SECONDS = 10

def classify_http(status):
    if status == 200:
        return "HEALTHY"
    if status in (401, 403):
        return "AUTH_CONFIG"
    if status == 404:
        return "ENDPOINT_CONFIG"
    if status == 429:
        return "RATE_LIMIT"
    if 500 <= status <= 599:
        return "SERVICE_ERROR"
    return "HTTP_ERROR"

def probe(url, opener=urllib.request.urlopen, attempts=MAX_ATTEMPTS, sleeper=time.sleep):
    if not isinstance(url, str) or not url.startswith("https://"):
        return {"status": "CONFIG_ERROR", "attempts": 0}
    attempts = max(1, min(int(attempts), MAX_ATTEMPTS))
    result = "UNKNOWN"
    for number in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"kenbot-safe-monitor/2"})
            with opener(req, timeout=TIMEOUT_SECONDS) as response:
                result = classify_http(response.status)
        except urllib.error.HTTPError as exc:
            result = classify_http(exc.code)
        except (urllib.error.URLError, TimeoutError, OSError):
            result = "NETWORK_ERROR"
        if result == "HEALTHY":
            return {"status": result, "attempts": number}
        if result in ("AUTH_CONFIG", "ENDPOINT_CONFIG", "CONFIG_ERROR"):
            break
        if number < attempts:
            sleeper(2)
    return {"status": result, "attempts": number}

def check_services(services, opener=urllib.request.urlopen, sleeper=time.sleep):
    return {name: probe(url, opener=opener, sleeper=sleeper) for name, url in services.items()}

def main():
    # Additional service endpoints require explicit opt-in; never touch data or deploy.
    services = dict(DEFAULT_SERVICES)
    extra = os.getenv("KENBOT_EXTRA_HEALTH_URLS_JSON", "").strip()
    if extra:
        try:
            parsed = json.loads(extra)
            if not isinstance(parsed, dict) or not all(isinstance(k,str) and isinstance(v,str) for k,v in parsed.items()):
                raise ValueError("Expected object of service-name -> HTTPS health URL")
            services.update(parsed)
        except (ValueError, TypeError) as exc:
            print("CONFIG_ERROR:", exc)
            return 2
    result = check_services(services)
    print(json.dumps(result, sort_keys=True))
    # Fails only on confirmed unhealthy state; never starts repair loops.
    return 0 if all(x["status"] == "HEALTHY" for x in result.values()) else 1

if __name__ == "__main__":
    sys.exit(main())
