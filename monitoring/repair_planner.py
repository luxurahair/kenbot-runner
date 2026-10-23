"""Fail-closed repair decision engine for Kenbot.

This module never deploys, edits Git, restarts a Mac, or changes customer data.
An external executor must independently authenticate, lock, approve and verify actions.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

MAX_OBSERVATION_AGE_SECONDS = 600
MIN_CONFIRMATION_GAP_SECONDS = 60
MAX_CONFIRMATION_GAP_SECONDS = 600
COOLDOWN_SECONDS = 3600
MAX_ATTEMPTS_PER_DAY = 2
FAULTS = frozenset({"SERVICE_ERROR", "NETWORK_ERROR"})
CONFIG_FAULTS = frozenset({"AUTH_CONFIG", "ENDPOINT_CONFIG", "CONFIG_ERROR", "RATE_LIMIT"})
SAFE_TARGETS = frozenset({"kenbot-api", "mac-remote"})

@dataclass(frozen=True)
class Observation:
    service: str
    status: str
    observed_at: datetime
    source: str = "kenbot-health"

@dataclass(frozen=True)
class Decision:
    state: str
    action: str
    reason: str

def _utc(t: datetime) -> datetime:
    if not isinstance(t, datetime) or t.tzinfo is None:
        raise ValueError("All timestamps must include timezone")
    return t.astimezone(timezone.utc)

def plan_repair(
    service: str,
    observations: Iterable[Observation],
    *,
    now: datetime,
    last_attempt: datetime | None = None,
    attempts_in_24h: int = 0,
    rescue_online: bool = False,
    cloud_codex_available: bool = False,
    approval: bool = False,
) -> Decision:
    """Compute a plan only; no side effects and no hidden approval defaults."""
    now = _utc(now)
    if service not in SAFE_TARGETS:
        return Decision("BLOCKED", "none", "Unknown or non-allowlisted service")
    if not isinstance(attempts_in_24h, int) or attempts_in_24h < 0:
        return Decision("BLOCKED", "none", "Invalid attempt count")
    if attempts_in_24h >= MAX_ATTEMPTS_PER_DAY:
        return Decision("COOLDOWN", "none", "Daily attempt budget exhausted")
    if last_attempt is not None:
        try:
            elapsed = (now - _utc(last_attempt)).total_seconds()
        except ValueError:
            return Decision("BLOCKED", "none", "Invalid last attempt timestamp")
        if elapsed < COOLDOWN_SECONDS:
            return Decision("COOLDOWN", "none", "Recovery cooldown active")

    recent = []
    try:
        for o in observations:
            if o.service != service or not o.source:
                continue
            age = (now - _utc(o.observed_at)).total_seconds()
            if 0 <= age <= MAX_OBSERVATION_AGE_SECONDS:
                recent.append(o)
    except (ValueError, TypeError, AttributeError):
        return Decision("BLOCKED", "none", "Invalid observation")
    recent.sort(key=lambda o: o.observed_at)

    if not recent:
        return Decision("OBSERVE", "none", "No fresh evidence")
    latest = recent[-1]
    if latest.status == "HEALTHY":
        return Decision("HEALTHY", "none", "Last check healthy")
    if latest.status in CONFIG_FAULTS:
        return Decision("INVESTIGATE", "none", "Authentication, endpoint or rate-limit issue")
    if latest.status not in FAULTS:
        return Decision("INVESTIGATE", "none", "Unclassified failure")
    if len(recent) < 2:
        return Decision("OBSERVE", "none", "Need another timed observation")
    prev = recent[-2]
    gap = (_utc(latest.observed_at) - _utc(prev.observed_at)).total_seconds()
    if prev.status not in FAULTS or not (MIN_CONFIRMATION_GAP_SECONDS <= gap <= MAX_CONFIRMATION_GAP_SECONDS):
        return Decision("OBSERVE", "none", "Failure not independently confirmed over time")

    if service == "mac-remote":
        if rescue_online:
            return Decision("DIAGNOSE", "rescue_diagnosis_only", "Request rescue diagnosis; never restart without its own safeguards")
        return Decision("ESCALATE", "none", "Mac and rescue channel not independently available")

    if not cloud_codex_available:
        return Decision("ESCALATE", "none", "Codex cloud unavailable; manual investigation required")
    if not approval:
        return Decision("REVIEW", "codex_diagnosis_only", "Prepare Codex diagnosis; production recovery not approved")
    return Decision("REVIEW", "request_operator_approved_recovery", "Evidence confirmed; executor must verify lock, target, and recovery result")
