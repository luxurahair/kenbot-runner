import unittest
from datetime import datetime, timedelta, timezone
from monitoring.guarded_render_recovery import can_restart, eligible_service, run, SERVICE_ID

NOW = datetime(2026,10,23,20,tzinfo=timezone.utc)
SERVICE = {"id":SERVICE_ID,"name":"kenbot-dashboard-api","type":"web_service","repo":"https://github.com/luxurahair/kenebec-ai","suspended":"not_suspended"}
def history(hours_ago=5,status="live"):
    return [{"deploy":{"createdAt":(NOW-timedelta(hours=hours_ago)).isoformat(),"status":status}}]
def failing(_): return {"status":"SERVICE_ERROR"}
def healthy(_): return {"status":"HEALTHY"}
def api_ok(method,path,token):
    if method=="POST": return {"status":"ok"}
    return SERVICE if path.startswith("/services/") and "/deploys" not in path else history()
class GuardedRecoveryTests(unittest.TestCase):
    def test_no_history(self): self.assertFalse(can_restart([],NOW)[0])
    def test_recent_deploy_cooldown(self): self.assertFalse(can_restart(history(0.1),NOW)[0])
    def test_old_deploy_allowed(self): self.assertTrue(can_restart(history(3),NOW)[0])
    def test_in_progress_deploy_blocked(self): self.assertFalse(can_restart(history(3,"build_in_progress"),NOW)[0])
    def test_two_recent_deploys_blocked(self): self.assertFalse(can_restart(history(3)+history(4),NOW)[0])
    def test_invalid_history(self): self.assertFalse(can_restart([{"deploy":{"createdAt":"bad","status":"live"}}],NOW)[0])
    def test_identity_ok(self): self.assertTrue(eligible_service(SERVICE))
    def test_identity_wrong(self): self.assertFalse(eligible_service(dict(SERVICE,id="wrong")))
    def test_identity_wrong_repo(self): self.assertFalse(eligible_service(dict(SERVICE,repo="https://github.com/other/repo")))
    def test_identity_suspended(self): self.assertFalse(eligible_service(dict(SERVICE,suspended="suspended")))
    def test_healthy_no_api(self):
        def forbidden(*args): raise AssertionError("No API call expected")
        self.assertEqual(run(enabled=True,token="fake",api=forbidden,check=healthy,now=NOW)[0],"HEALTHY")
    def test_disabled_no_restart(self):
        self.assertEqual(run(enabled=False,check=failing,sleeper=lambda _:None,now=NOW)[0],"DISABLED")
    def test_missing_token(self):
        self.assertEqual(run(enabled=True,check=failing,sleeper=lambda _:None,now=NOW)[0],"BLOCKED")
    def test_approved_one_restart(self):
        calls=[]
        def api(method,path,token):
            calls.append(method)
            return api_ok(method,path,token)
        self.assertEqual(run(enabled=True,token="fake",api=api,check=failing,sleeper=lambda _:None,now=NOW)[0],"RESTART_REQUESTED")
        self.assertEqual(calls,["GET","GET","POST"])
    def test_api_error_blocked(self):
        def bad(*args): raise ValueError("offline")
        self.assertEqual(run(enabled=True,token="fake",api=bad,check=failing,sleeper=lambda _:None,now=NOW)[0],"BLOCKED")
    def test_no_restart_on_auth(self):
        self.assertEqual(run(enabled=True,token="fake",check=lambda _:{"status":"AUTH_CONFIG"},now=NOW)[0],"INVESTIGATE")
    def test_recovery_between_checks(self):
        calls=iter([{"status":"SERVICE_ERROR"},{"status":"HEALTHY"}])
        self.assertEqual(run(check=lambda _:next(calls),sleeper=lambda _:None,now=NOW)[0],"RECOVERED")
    def test_recovery_before_restart(self):
        calls=iter([{"status":"SERVICE_ERROR"},{"status":"SERVICE_ERROR"},{"status":"HEALTHY"}])
        self.assertEqual(run(enabled=True,token="fake",api=api_ok,check=lambda _:next(calls),sleeper=lambda _:None,now=NOW)[0],"RECOVERED")
if __name__=="__main__": unittest.main()
