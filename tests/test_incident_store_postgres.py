"""Real PostgreSQL integration checks. Runs only against disposable CI database."""
import os
import threading
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

try:
    import psycopg
except ImportError:
    psycopg = None

from monitoring.incident_store import claim_attempt, read_history, finish_attempt

DSN = os.getenv("KENBOT_TEST_DATABASE_URL")
NOW = datetime(2026,10,23,20,tzinfo=timezone.utc)

@unittest.skipUnless(DSN and psycopg, "Disposable PostgreSQL required")
class RealPostgresLeaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not DSN or not psycopg:return
        with psycopg.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE kenbot_control.recovery_attempts, kenbot_control.incidents, kenbot_control.recovery_leases CASCADE")
                cls.incident = uuid4()
                cur.execute(
                    """INSERT INTO kenbot_control.incidents
                       (incident_id,service_id,environment,tenant_scope,fingerprint,state,first_seen,last_seen)
                       VALUES (%s,%s,%s,%s,%s,'OPEN',%s,%s)""",
                    (cls.incident,"kenbot-api","staging","kennebec","test-fingerprint",NOW,NOW))
    def setUp(self):
        with psycopg.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE kenbot_control.recovery_attempts, kenbot_control.recovery_leases")
    def claim(self, at=NOW):
        with psycopg.connect(DSN) as conn:
            return claim_attempt(conn,self.incident,"kenbot-api","staging","kennebec",
                                 "request_safe_restart","ci-test",now=at)
    def test_atomic_single_winner_eight_connections(self):
        gate=threading.Barrier(8)
        results=[]
        errors=[]
        def worker():
            try:
                gate.wait(timeout=5)
                results.append(self.claim())
            except Exception as exc:
                errors.append(str(exc))
        threads=[threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(timeout=12)
        self.assertFalse(errors,errors)
        self.assertEqual(len(results),8)
        self.assertEqual(sum(r is not None for r in results),1)
        with psycopg.connect(DSN) as conn:
            history=read_history(conn,"kenbot-api","staging","kennebec",now=NOW)
            self.assertEqual(history.attempts_in_24h,1)
    def test_lease_expiry_fencing_and_attempt_budget(self):
        a=self.claim()
        self.assertIsNotNone(a)
        b=self.claim(NOW+timedelta(minutes=71))
        self.assertIsNotNone(b)
        self.assertGreater(b["fencing_token"],a["fencing_token"])
        with psycopg.connect(DSN) as conn:
            self.assertFalse(finish_attempt(conn,a,"VERIFIED",now=NOW+timedelta(minutes=72)))
            self.assertTrue(finish_attempt(conn,b,"VERIFIED",now=NOW+timedelta(minutes=72)))
        self.assertIsNone(self.claim(NOW+timedelta(hours=3)))
if __name__=="__main__":unittest.main()
