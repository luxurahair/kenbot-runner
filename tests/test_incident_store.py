"""Offline contract and race tests. Simulated DB, no Supabase writes."""
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from monitoring.incident_store import read_history, claim_attempt, finish_attempt
from monitoring.incident_decision_cli import decide_with_connection

NOW=datetime(2026,10,23,20,tzinfo=timezone.utc)
class Store:
    def __init__(self):
        self.lock=threading.RLock()
        self.attempts=[]
        self.lease=None
        self.fence=0
class Cursor:
    def __init__(self,db): self.db=db;self.row=None
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,sql,args):
        s=" ".join(sql.lower().split())
        db=self.db.store
        if "pg_try_advisory_xact_lock" in s:
            self.row=(True,)
        elif "select count(*), max(started_at)" in s:
            service,env,tenant,lower,upper=args
            matches=[x for x in db.attempts if x["service"]==service and x["env"]==env
                     and x["tenant"]==tenant and lower<=x["at"]<=upper]
            self.row=(len(matches),max((x["at"] for x in matches),default=None))
        elif "insert into kenbot_control.recovery_leases" in s:
            service,env,tenant,lease_id,owner,expires,at,cutoff=args
            if db.lease and db.lease["expires"]>=cutoff:
                self.row=None
            else:
                db.fence+=1
                db.lease={"id":lease_id,"expires":expires,"fence":db.fence}
                self.row=(db.fence,)
        elif "insert into kenbot_control.recovery_attempts" in s:
            attempt_id,incident,service,env,tenant,action,executor,lease_id,fence,at=args
            db.attempts.append({"id":attempt_id,"service":service,"env":env,
                                "tenant":tenant,"lease":lease_id,"fence":fence,
                                "at":at,"result":"STARTED"})
            self.row=None
        elif "update kenbot_control.recovery_attempts" in s:
            result,at,attempt_id,lease_id,fence,now=args
            matches=[x for x in db.attempts if x["id"]==attempt_id and x["result"]=="STARTED"]
            valid=(db.lease and db.lease["id"]==lease_id and db.lease["fence"]==fence
                   and db.lease["expires"]>now)
            if matches and valid:
                matches[0]["result"]=result
                self.row=(attempt_id,)
            else:self.row=None
        else:raise AssertionError("Unexpected query")
    def fetchone(self):return self.row
class DB:
    def __init__(self,store):self.store=store
    @contextmanager
    def transaction(self):
        with self.store.lock:yield
    def cursor(self):return Cursor(self)
def claim(db,now=NOW,action="request_safe_restart"):
    return claim_attempt(db,"incident-1","kenbot-api","staging","kennebec",
                         action,"codex-service",now=now)
class DurableStoreTests(unittest.TestCase):
    def setUp(self):self.store=Store();self.db=DB(self.store)
    def test_empty_history(self):
        h=read_history(self.db,"kenbot-api","staging","kennebec",now=NOW)
        self.assertEqual(h.attempts_in_24h,0)
    def test_first_claim(self):self.assertIsNotNone(claim(self.db))
    def test_second_claim_blocked(self):
        self.assertIsNotNone(claim(self.db))
        self.assertIsNone(claim(self.db))
    def test_count_persists(self):
        claim(self.db)
        h=read_history(self.db,"kenbot-api","staging","kennebec",now=NOW)
        self.assertEqual(h.attempts_in_24h,1)
    def test_lease_expiry_allows_second_after_cooldown(self):
        claim(self.db)
        self.assertIsNotNone(claim(self.db,NOW+timedelta(minutes=71)))
    def test_new_claim_after_cooldown(self):
        claim(self.db)
        self.assertIsNotNone(claim(self.db,NOW+timedelta(hours=2)))
    def test_budget_blocks_third(self):
        claim(self.db)
        claim(self.db,NOW+timedelta(hours=2))
        self.assertIsNone(claim(self.db,NOW+timedelta(hours=4)))
    def test_lease_generation(self):
        a=claim(self.db);b=claim(self.db,NOW+timedelta(hours=2))
        self.assertGreater(b["fencing_token"],a["fencing_token"])
    def test_stale_lease_cannot_finish(self):
        a=claim(self.db);claim(self.db,NOW+timedelta(hours=2))
        self.assertFalse(finish_attempt(self.db,a,"VERIFIED",now=NOW+timedelta(hours=2)))
    def test_finish_verified(self):
        a=claim(self.db)
        self.assertTrue(finish_attempt(self.db,a,"VERIFIED",now=NOW+timedelta(minutes=1)))
    def test_double_finish_rejected(self):
        a=claim(self.db)
        self.assertTrue(finish_attempt(self.db,a,"FAILED",now=NOW+timedelta(minutes=1)))
        self.assertFalse(finish_attempt(self.db,a,"VERIFIED",now=NOW+timedelta(minutes=2)))
    def test_unknown_action(self):
        with self.assertRaises(ValueError):claim(self.db,action="force_push")
    def test_bad_environment(self):
        with self.assertRaises(ValueError):
            read_history(self.db,"kenbot-api","invalid","kennebec",now=NOW)
    def test_history_scope_isolated(self):
        claim(self.db)
        self.assertEqual(read_history(self.db,"kenbot-api","staging","other",now=NOW).attempts_in_24h,0)
    def test_untrusted_json_history_ignored(self):
        p={"schema_version":"kenbot.probe.v1","service":"kenbot-api",
           "observations":[{"status":"SERVICE_ERROR","source":"probe","observed_at":(NOW-timedelta(seconds=120)).isoformat()},
                           {"status":"SERVICE_ERROR","source":"probe","observed_at":NOW.isoformat()}],
           "history":{"verified":True,"source":"verified-incident-store","attempts_in_24h":0},
           "capabilities":{"cloud_codex_available":True}}
        from monitoring.decision_adapter import decide_envelope
        self.assertEqual(decide_envelope(p,now=NOW)["action"],"blocked")
        self.assertEqual(decide_with_connection(p,self.db,environment="staging",tenant_scope="kennebec",now=NOW)["action"],"diagnose_codex")
    def test_parallel_only_one_claim(self):
        gate=threading.Barrier(8)
        results=[]
        def worker():
            gate.wait()
            value=claim(DB(self.store))
            results.append(value)
        threads=[threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=3)
        self.assertEqual(len(results),8)
        self.assertEqual(sum(x is not None for x in results),1)
        self.assertEqual(len(self.store.attempts),1)
    def test_missing_time_zone(self):
        with self.assertRaises(ValueError):claim(self.db,datetime(2026,10,23))
if __name__=="__main__":unittest.main()
