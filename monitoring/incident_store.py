"""Private Postgres incident history and atomic recovery leases.

No connection is made on import. Requires a dedicated server-side psycopg
connection, never a browser key or a JSON 'verified' flag.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

MAX_ATTEMPTS = 2
COOLDOWN = timedelta(hours=1)
LEASE_MINUTES = 70

@dataclass(frozen=True)
class VerifiedHistory:
    service: str
    environment: str
    tenant_scope: str
    attempts_in_24h: int
    last_attempt_at: datetime | None

def utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware datetime required")
    return value.astimezone(timezone.utc)

def validate_scope(service, environment, tenant):
    if not all(isinstance(v,str) and v and len(v)<=128 for v in (service,environment,tenant)):
        raise ValueError("Invalid incident scope")
    if environment not in ("production","staging"):
        raise ValueError("Unrecognized environment")

def read_history(connection, service, environment, tenant_scope, *, now=None):
    """Read history from an authenticated database connection, never an artifact."""
    validate_scope(service, environment, tenant_scope)
    now = utc(now or datetime.now(timezone.utc))
    with connection.cursor() as cur:
        cur.execute(
            """SELECT count(*), max(started_at)
               FROM kenbot_control.recovery_attempts
               WHERE service_id=%s AND environment=%s AND tenant_scope=%s
                 AND started_at >= %s AND started_at <= %s""",
            (service,environment,tenant_scope,now-timedelta(hours=24),now))
        row = cur.fetchone()
    if row is None or len(row)!=2:
        raise ValueError("Incomplete incident history")
    count, last = row
    if type(count) is not int or count<0:
        raise ValueError("Invalid history count")
    last = utc(last) if last is not None else None
    return VerifiedHistory(service,environment,tenant_scope,count,last)

def claim_attempt(connection, incident_id, service, environment, tenant_scope,
                  action, executor_identity, *, now=None):
    """Atomic, fail-closed lease and attempt registration. Never performs repair.

    Transaction-scoped advisory lock serializes attempts for the same scope.
    A committed attempt counts toward the budget even if its action later fails.
    """
    validate_scope(service,environment,tenant_scope)
    if action not in ("request_safe_restart","rescue_diagnosis_only"):
        raise ValueError("Action not allowlisted")
    if not isinstance(executor_identity,str) or not executor_identity:
        raise ValueError("Executor identity required")
    now = utc(now or datetime.now(timezone.utc))
    lease_id, attempt_id = str(uuid4()),str(uuid4())
    key = service+"|"+environment+"|"+tenant_scope
    with connection.transaction():
        with connection.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",(key,))
            locked = cur.fetchone()
            if not locked or locked[0] is not True:
                return None
            cur.execute(
                """SELECT count(*), max(started_at)
                   FROM kenbot_control.recovery_attempts
                   WHERE service_id=%s AND environment=%s AND tenant_scope=%s
                     AND started_at >= %s AND started_at <= %s""",
                (service,environment,tenant_scope,now-timedelta(hours=24),now))
            count,last = cur.fetchone()
            if type(count) is not int or count >= MAX_ATTEMPTS:
                return None
            if last is not None and now-utc(last)<COOLDOWN:
                return None
            cur.execute(
                """INSERT INTO kenbot_control.recovery_leases
                   (service_id,environment,tenant_scope,lease_id,owner,expires_at,heartbeat_at,fencing_token)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,1)
                   ON CONFLICT (service_id,environment,tenant_scope) DO UPDATE
                   SET lease_id=EXCLUDED.lease_id, owner=EXCLUDED.owner,
                       expires_at=EXCLUDED.expires_at, heartbeat_at=EXCLUDED.heartbeat_at,
                       fencing_token=kenbot_control.recovery_leases.fencing_token+1
                   WHERE kenbot_control.recovery_leases.expires_at < %s
                   RETURNING fencing_token""",
                (service,environment,tenant_scope,lease_id,executor_identity,
                 now+timedelta(minutes=LEASE_MINUTES),now,now))
            fence = cur.fetchone()
            if fence is None:
                return None
            cur.execute(
                """INSERT INTO kenbot_control.recovery_attempts
                   (attempt_id,incident_id,service_id,environment,tenant_scope,action,
                    executor_identity,lease_id,fencing_token,started_at,result)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'STARTED')""",
                (attempt_id,incident_id,service,environment,tenant_scope,action,
                 executor_identity,lease_id,fence[0],now))
    return {"attempt_id":attempt_id,"lease_id":lease_id,"fencing_token":fence[0],
            "service":service,"environment":environment,"tenant_scope":tenant_scope}

def finish_attempt(connection, claim, result, *, now=None):
    """Record outcome only if the lease generation still belongs to this attempt."""
    if result not in ("VERIFIED","FAILED","UNKNOWN"):
        raise ValueError("Invalid result")
    now = utc(now or datetime.now(timezone.utc))
    with connection.transaction():
        with connection.cursor() as cur:
            cur.execute(
                """UPDATE kenbot_control.recovery_attempts AS a
                   SET result=%s, finished_at=%s
                   WHERE a.attempt_id=%s AND a.result='STARTED'
                     AND EXISTS (
                       SELECT 1 FROM kenbot_control.recovery_leases AS l
                       WHERE l.service_id=a.service_id AND l.environment=a.environment
                         AND l.tenant_scope=a.tenant_scope AND l.lease_id=%s
                         AND l.fencing_token=%s AND l.expires_at>%s)
                   RETURNING attempt_id""",
                (result,now,claim["attempt_id"],claim["lease_id"],claim["fencing_token"],now))
            return cur.fetchone() is not None
