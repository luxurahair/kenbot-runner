# Kenbot V3 decision interface (preview)

The single decision engine is `monitoring/repair_planner.py`. The read-only adapter `monitoring/decision_adapter.py` turns a versioned probe envelope into a V3 JSON artifact. It never dispatches Codex, restarts Render or Mac, modifies Git, or writes repair history.

## Input

```json
{
  "schema_version": "kenbot.probe.v1",
  "service": "kenbot-api",
  "observations": [
    {"status": "SERVICE_ERROR", "source": "render-health", "observed_at": "2026-10-23T19:57:00Z"},
    {"status": "SERVICE_ERROR", "source": "render-health", "observed_at": "2026-10-23T19:59:00Z"}
  ],
  "history": {
    "source": "verified-incident-store",
    "verified": true,
    "attempts_in_24h": 0,
    "last_attempt_at": null
  },
  "capabilities": {"cloud_codex_available": true, "rescue_online": false}
}
```

The `history.verified` field is a **caller assertion, not a cryptographic proof**. Only a future authenticated incident-store reader may produce it. A user-supplied file must never authorize an action. The adapter deliberately ignores `approval` and always emits `execution_allowed=false`.

Run: `python -m monitoring.decision_adapter probe.json repair-decision.json`.

## Output

`schema_version`, `timestamp`, `service`, `severity`, `state`, `action`, `reason`, `evidence_count`, `history_verified`, `execution_allowed=false`, `executor_approval_required=true`.

Possible `action` values: `none`, `observe`, `diagnose_codex`, `request_safe_restart`, `create_draft_pr`, `notify_only`, `blocked`. The current read-only adapter **never emits write-capable actions** `request_safe_restart` or `create_draft_pr`; those require separately authenticated executors and policies. For Mac Rescue diagnosis, it emits `notify_only` rather than pretending a Mac command has been sent.

The CI job `V3 decision artifact (synthetic fixture only)` generates a clearly labeled **synthetic healthy example** and uploads it for 14 days. This is not a live production probe or an incident log. Real probe ingestion and durable, authenticated history are not yet connected.

## Security and next steps

- Missing history blocks escalation to Codex diagnosis; auth/config/429 never restart.
- Reject malformed inputs; never write a decision artifact on invalid input.
- Integrate trusted, durable incident history, immutable attempt records and a distributed lease before permitting any action.
- Keep `monitoring/guarded_render_recovery.py` separate as the existing bounded Render recovery executor; it does not consume this V3 artifact.
- Test a genuine incident and verified recovery before declaring V3 operational.
