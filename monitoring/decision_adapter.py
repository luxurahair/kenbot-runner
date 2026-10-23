"""V3 JSON decision adapter: uses V2 as the only decision engine, never acts."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from monitoring.repair_planner import Observation, plan_repair

SCHEMA = "kenbot.repair-decision.v3"
ACTIONS = {"none", "observe", "diagnose_codex", "request_safe_restart",
           "create_draft_pr", "notify_only", "blocked"}

def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Missing timestamp")
    t = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if t.tzinfo is None:
        raise ValueError("Naive timestamp")
    return t.astimezone(timezone.utc)

def decide_envelope(probe, now=None):
    if not isinstance(probe, dict) or probe.get("schema_version") != "kenbot.probe.v1":
        raise ValueError("Wrong probe schema")
    service = probe.get("service")
    if not isinstance(service, str) or not service:
        raise ValueError("Invalid service")
    raw = probe.get("observations")
    if not isinstance(raw, list) or len(raw) > 100:
        raise ValueError("Invalid observations")
    observations = []
    for item in raw:
        if not isinstance(item, dict) or not isinstance(item.get("status"), str):
            raise ValueError("Invalid observation")
        if not isinstance(item.get("source"), str) or not item["source"]:
            raise ValueError("Missing observation source")
        observations.append(Observation(service, item["status"],
                                        timestamp(item.get("observed_at")), item["source"]))
    history = probe.get("history")
    verified = (isinstance(history, dict) and history.get("verified") is True
                and history.get("source") in ("verified-incident-store", "verified-render-deploy-history"))
    attempts = 0
    last = None
    if verified:
        attempts = history.get("attempts_in_24h")
        if type(attempts) is not int or attempts < 0:
            raise ValueError("Invalid attempts")
        if history.get("last_attempt_at") is not None:
            last = timestamp(history["last_attempt_at"])
    caps = probe.get("capabilities", {})
    if not isinstance(caps, dict):
        raise ValueError("Invalid capabilities")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("Naive current time")
    # Input-supplied approval is deliberately ignored. Never authorize a write.
    decision = plan_repair(service, observations, now=now,
                           last_attempt=last, attempts_in_24h=attempts,
                           rescue_online=caps.get("rescue_online") is True,
                           cloud_codex_available=caps.get("cloud_codex_available") is True,
                           approval=False)
    action = {"HEALTHY":"none", "OBSERVE":"observe", "INVESTIGATE":"notify_only",
              "COOLDOWN":"blocked", "BLOCKED":"blocked", "ESCALATE":"notify_only",
              "DIAGNOSE":"notify_only", "REVIEW":"diagnose_codex"}.get(decision.state, "blocked")
    reason = decision.reason
    if not verified and action == "diagnose_codex":
        action, reason = "blocked", "Durable history not independently verified"
    if action not in ACTIONS:
        raise ValueError("Invalid decision action")
    latest = sorted(observations, key=lambda x:x.observed_at)[-1].status if observations else ""
    severity = ("Info" if decision.state == "HEALTHY" else
                "Critical" if latest in ("SERVICE_ERROR", "NETWORK_ERROR") else "Warning")
    return {"schema_version":SCHEMA, "timestamp":now.astimezone(timezone.utc).isoformat(),
            "service":service, "severity":severity, "state":decision.state,
            "action":action, "reason":reason, "evidence_count":len(observations),
            "history_verified":verified, "execution_allowed":False,
            "executor_approval_required":True}

def main(args=None):
    args = sys.argv[1:] if args is None else args
    if len(args) != 2:
        print("Usage: python -m monitoring.decision_adapter probe.json decision.json",file=sys.stderr)
        return 2
    try:
        source, destination = map(Path, args)
        if source.stat().st_size > 131072:
            raise ValueError("Oversized probe")
        decision = decide_envelope(json.loads(source.read_text(encoding="utf-8")))
        destination.write_text(json.dumps(decision, indent=2, sort_keys=True)+"\n",encoding="utf-8")
        print(json.dumps({"action":decision["action"],"execution_allowed":False}))
        return 0
    except (OSError, ValueError, TypeError, KeyError):
        print("Invalid probe; no action authorized",file=sys.stderr)
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
