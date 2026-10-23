# Recovery planner V2

This phase adds a read-only decision engine for Codex Cloud and Mac Rescue. It does not restart Render or Mac services, change customer data, merge pull requests, or restore backups. A separate executor requires authenticated approvals, persistent cooldowns, an exclusive lease and post-recovery health checks before production activation.

Run offline tests: `python -m unittest discover -s tests -p test_repair_planner.py -v`.
