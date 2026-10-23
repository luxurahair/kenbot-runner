# Kenbot Recovery V2 — operational contract

The recovery runner is `monitoring/guarded_render_recovery.py`. It is connected to the existing 15-minute GitHub Actions monitor. The default is **disabled** until a trusted operator explicitly sets GitHub Actions repository variable `KENBOT_AUTOREPAIR_ENABLED=1` and configures the repository secret `RENDER_API_KEY`.

## Narrow automatic action
- Target **only** Render web service `kenbot-dashboard-api`, ID `srv-d7da02tckfvc73en2g9g`. Validate its Render service ID, type, name, repository and unsuspended status.
- Confirm health endpoint failures twice, at least 65 seconds apart, and again immediately before restart. Authentication errors, 404, 429 and healthy responses never cause a restart.
- Inspect Render deployment history before a restart: block in-progress deployments, require at least one deploy record, allow at most 2 deploys in 24 hours (conservative total), and wait at least one hour since any deploy.
- Make at most one authenticated `POST /v1/services/{id}/restart` request. Wait 45 seconds and check health again; an unverified recovery is a failing workflow, not success.
- GitHub Actions concurrency prevents overlap between monitor runs. GitHub job timeout is five minutes.
- Never use Vercel deploy hooks, empty commits, `git push --force`, backups or database restoration as recovery actions.

## Mac Remote and Codex
`monitoring/repair_planner.py` plans diagnoses for Codex Cloud and Mac Rescue without side effects. **No Mac restart or Codex task dispatch is automatically enabled by this change.** The independent Mac Rescue service must diagnose and require its own authenticated authorization before a restart. Cloud Codex is available as a separate diagnosis workflow in `luxurahair/kenebec-ai`.

## Verification
`python -m unittest discover -s tests -p test_repair_planner.py -v`
`python -m unittest discover -s tests -p test_guarded_render_recovery.py -v`

The workflow also runs the pre-existing monitor, archive and safety contract tests. Tests are offline with mocked Render calls and cannot restart production.

**Production acceptance** requires an observed real failure and verified recovery, without deliberately breaking production. A green unit test or green health check alone is not proof of self-healing. The backup cron and 180-day Google Drive/Supabase retention are separate and are not modified.
