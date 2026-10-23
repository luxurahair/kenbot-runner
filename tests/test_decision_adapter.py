"""Decision V3 interface tests. No production API calls."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from monitoring.decision_adapter import decide_envelope, main

NOW = datetime(2026,10,23,20,tzinfo=timezone.utc)
def envelope(statuses, *, history=True, service="kenbot-api"):
    obs = [{"status":s,"source":"test-probe",
            "observed_at":(NOW-timedelta(seconds=120*(len(statuses)-1-i))).isoformat()}
           for i,s in enumerate(statuses)]
    return {"schema_version":"kenbot.probe.v1","service":service,"observations":obs,
            "capabilities":{"cloud_codex_available":True,"rescue_online":True},
            **({"history":{"source":"verified-incident-store","verified":True,
                           "attempts_in_24h":0,"last_attempt_at":None}} if history else {})}
def decision(p): return decide_envelope(p,now=NOW)
class AdapterTests(unittest.TestCase):
    def test_healthy(self): self.assertEqual(decision(envelope(["HEALTHY"]))["action"],"none")
    def test_single_failure(self): self.assertEqual(decision(envelope(["SERVICE_ERROR"]))["action"],"observe")
    def test_confirmed_failure(self): self.assertEqual(decision(envelope(["SERVICE_ERROR","SERVICE_ERROR"]))["action"],"diagnose_codex")
    def test_missing_history_blocks(self): self.assertEqual(decision(envelope(["SERVICE_ERROR","SERVICE_ERROR"],history=False))["action"],"blocked")
    def test_auth(self): self.assertEqual(decision(envelope(["AUTH_CONFIG"]))["action"],"notify_only")
    def test_rate_limit(self): self.assertEqual(decision(envelope(["RATE_LIMIT"]))["action"],"notify_only")
    def test_unknown_service(self): self.assertEqual(decision(envelope(["HEALTHY"],service="unknown"))["action"],"blocked")
    def test_cooldown(self):
        p=envelope(["SERVICE_ERROR","SERVICE_ERROR"])
        p["history"]["last_attempt_at"]=(NOW-timedelta(minutes=15)).isoformat()
        self.assertEqual(decision(p)["action"],"blocked")
    def test_attempt_budget(self):
        p=envelope(["SERVICE_ERROR","SERVICE_ERROR"])
        p["history"]["attempts_in_24h"]=2
        self.assertEqual(decision(p)["action"],"blocked")
    def test_never_executes(self):
        p=envelope(["SERVICE_ERROR","SERVICE_ERROR"])
        p["approval"]=True
        self.assertIs(decision(p)["execution_allowed"],False)
        self.assertNotEqual(decision(p)["action"],"request_safe_restart")
    def test_schema(self): self.assertEqual(decision(envelope(["HEALTHY"]))["schema_version"],"kenbot.repair-decision.v3")
    def test_bad_schema(self):
        p=envelope(["HEALTHY"]);p["schema_version"]="wrong"
        with self.assertRaises(ValueError):decision(p)
    def test_naive_timestamp(self):
        p=envelope(["HEALTHY"]);p["observations"][0]["observed_at"]="2026-10-23T20:00:00"
        with self.assertRaises(ValueError):decision(p)
    def test_bad_history_count(self):
        p=envelope(["HEALTHY"]);p["history"]["attempts_in_24h"]=-1
        with self.assertRaises(ValueError):decision(p)
    def test_bad_observations(self):
        p=envelope(["HEALTHY"]);p["observations"]="wrong"
        with self.assertRaises(ValueError):decision(p)
    def test_bad_capabilities(self):
        p=envelope(["HEALTHY"]);p["capabilities"]="wrong"
        with self.assertRaises(ValueError):decision(p)
    def test_cli_output(self):
        with tempfile.TemporaryDirectory() as d:
            src=Path(d)/"probe.json";dst=Path(d)/"decision.json"
            src.write_text(json.dumps(envelope(["HEALTHY"])))
            self.assertEqual(main([str(src),str(dst)]),0)
            self.assertIs(json.loads(dst.read_text())["execution_allowed"],False)
    def test_cli_invalid_no_artifact(self):
        with tempfile.TemporaryDirectory() as d:
            src=Path(d)/"probe.json";dst=Path(d)/"decision.json"
            src.write_text("{}")
            self.assertEqual(main([str(src),str(dst)]),2)
            self.assertFalse(dst.exists())
if __name__=="__main__": unittest.main()
