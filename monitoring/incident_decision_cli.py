"""Read an authenticated incident history before V3 decision generation.

Opt-in CLI only. No GitHub Actions or production job calls this module yet.
Requires psycopg 3 and a dedicated private database identity.
"""
import json
import os
import sys
from pathlib import Path
from monitoring.decision_adapter import decide_envelope
from monitoring.incident_store import read_history

def decide_with_connection(probe, connection, *, environment, tenant_scope, now=None):
    if not isinstance(probe,dict):
        raise ValueError("Invalid probe")
    # Environment and tenant are provided by the trusted executor, never probe JSON.
    history = read_history(connection, probe.get("service"), environment, tenant_scope, now=now)
    return decide_envelope(probe, now=now, trusted_history=history)

def main(args=None, *, connect=None):
    args = sys.argv[1:] if args is None else args
    if len(args) != 2:
        print("Usage: python -m monitoring.incident_decision_cli PROBE.json DECISION.json",file=sys.stderr)
        return 2
    dsn = os.getenv("KENBOT_INCIDENT_DATABASE_URL")
    env = os.getenv("KENBOT_EXECUTION_ENV")
    tenant = os.getenv("KENBOT_TENANT_SCOPE")
    if not dsn or not env or not tenant:
        print("Private incident-store connection/scope not configured",file=sys.stderr)
        return 2
    try:
        source, destination = map(Path,args)
        if source.stat().st_size > 131072:
            raise ValueError("Oversized probe")
        probe = json.loads(source.read_text(encoding="utf-8"))
        if connect is None:
            import psycopg
            connect = psycopg.connect
        with connect(dsn,connect_timeout=5) as connection:
            result = decide_with_connection(probe,connection,environment=env,tenant_scope=tenant)
        destination.write_text(json.dumps(result,sort_keys=True,indent=2)+"\n",encoding="utf-8")
        print(json.dumps({"action":result["action"],"execution_allowed":False}))
        return 0
    except Exception:
        # No connection string, exception details or raw incident payload in logs.
        print("Incident-store verification failed; no action authorized",file=sys.stderr)
        return 2
if __name__ == "__main__":
    raise SystemExit(main())
