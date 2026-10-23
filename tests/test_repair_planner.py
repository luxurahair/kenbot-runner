import unittest
from datetime import datetime, timedelta, timezone
from monitoring.repair_planner import Observation, plan_repair

NOW = datetime(2026, 10, 23, 16, tzinfo=timezone.utc)
def o(status, age, service="kenbot-api"):
    return Observation(service, status, NOW-timedelta(seconds=age))
def decide(items, **kw):
    return plan_repair("kenbot-api", items, now=NOW, cloud_codex_available=True, **kw)

class RepairPlannerTests(unittest.TestCase):
    def test_no_evidence(self): self.assertEqual(decide([]).state, "OBSERVE")
    def test_healthy(self): self.assertEqual(decide([o("HEALTHY",0)]).state, "HEALTHY")
    def test_one_failure(self): self.assertEqual(decide([o("SERVICE_ERROR",0)]).state, "OBSERVE")
    def test_two_failures(self): self.assertEqual(decide([o("SERVICE_ERROR",120),o("SERVICE_ERROR",0)]).action, "codex_diagnosis_only")
    def test_no_automatic_restart(self): self.assertEqual(decide([o("SERVICE_ERROR",120),o("SERVICE_ERROR",0)]).state, "REVIEW")
    def test_auth_error(self): self.assertEqual(decide([o("SERVICE_ERROR",120),o("AUTH_CONFIG",0)]).state, "INVESTIGATE")
    def test_rate_limit(self): self.assertEqual(decide([o("SERVICE_ERROR",120),o("RATE_LIMIT",0)]).state, "INVESTIGATE")
    def test_recent_recovery(self): self.assertEqual(decide([o("SERVICE_ERROR",120),o("HEALTHY",0)]).state, "HEALTHY")
    def test_too_close(self): self.assertEqual(decide([o("SERVICE_ERROR",30),o("SERVICE_ERROR",0)]).state, "OBSERVE")
    def test_stale(self): self.assertEqual(decide([o("SERVICE_ERROR",700)]).state, "OBSERVE")
    def test_daily_limit(self): self.assertEqual(decide([], attempts_in_24h=2).state, "COOLDOWN")
    def test_cooldown(self): self.assertEqual(decide([],last_attempt=NOW-timedelta(minutes=10)).state, "COOLDOWN")
    def test_unknown_target(self): self.assertEqual(plan_repair("customer-db", [], now=NOW).state, "BLOCKED")
    def test_cloud_unavailable(self):
        self.assertEqual(plan_repair("kenbot-api",[o("SERVICE_ERROR",120),o("SERVICE_ERROR",0)],now=NOW).state,"ESCALATE")
    def test_mac_rescue(self):
        evidence=[o("NETWORK_ERROR",120,"mac-remote"),o("NETWORK_ERROR",0,"mac-remote")]
        self.assertEqual(plan_repair("mac-remote",evidence,now=NOW,rescue_online=True).action,"rescue_diagnosis_only")
    def test_mac_offline(self):
        evidence=[o("NETWORK_ERROR",120,"mac-remote"),o("NETWORK_ERROR",0,"mac-remote")]
        self.assertEqual(plan_repair("mac-remote",evidence,now=NOW).state,"ESCALATE")
    def test_naive_timestamp(self):
        self.assertEqual(decide([Observation("kenbot-api","SERVICE_ERROR",datetime(2026,10,23))]).state,"BLOCKED")
    def test_approved_requires_executor(self):
        self.assertEqual(decide([o("SERVICE_ERROR",120),o("SERVICE_ERROR",0)],approval=True).action,"request_operator_approved_recovery")

if __name__ == "__main__":
    unittest.main()
