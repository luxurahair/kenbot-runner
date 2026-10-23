"""Read-only validation of existing Kennebec critical-feature backup archives."""
from __future__ import annotations
import hashlib
import json
import os
import tarfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

EXPECTED_PREFIXES = (
    "kenbot-dashboard/frontend/src/features/reprise-evaluations-grossistes/",
    "tests/test_critical_regression.py",
    "REPRISE_EVALUATIONS_LOCKED.md",
    "ARCHITECTURE_GROSSISTES_REPRISE_EVALUATIONS.md",
)

def safe_member(name: str) -> bool:
    p = PurePosixPath(name)
    return bool(name) and not p.is_absolute() and ".." not in p.parts and not name.startswith("./../") and any(
        name == prefix.rstrip("/") or name.startswith(prefix) for prefix in EXPECTED_PREFIXES
    )

def inspect_archive(path: str, max_age_hours: int = 72, now=None) -> dict:
    archive = Path(path)
    if not archive.is_file():
        return {"status": "MISSING", "name": archive.name}
    age = (now or datetime.now(timezone.utc)).timestamp() - archive.stat().st_mtime
    if age < -300:
        return {"status": "INVALID_TIMESTAMP", "name": archive.name}
    try:
        sha = hashlib.sha256()
        with archive.open("rb") as src:
            for part in iter(lambda: src.read(1024 * 1024), b""):
                sha.update(part)
        with tarfile.open(archive, mode="r:gz") as src:
            members = src.getmembers()
            if not members or any(not safe_member(m.name) or not (m.isfile() or m.isdir()) for m in members):
                return {"status": "UNSAFE", "name": archive.name}
            if not any(m.isfile() and m.name.startswith(EXPECTED_PREFIXES[0]) for m in members):
                return {"status": "INCOMPLETE", "name": archive.name}
            for member in members:
                if member.isfile():
                    stream = src.extractfile(member)
                    if stream is None:
                        return {"status": "CORRUPT", "name": archive.name}
                    while stream.read(1024 * 1024):
                        pass
        return {
            "status": "VERIFIED" if age <= max_age_hours * 3600 else "STALE",
            "name": archive.name,
            "sha256": sha.hexdigest(),
            "age_hours": round(age / 3600, 2),
            "members": len(members),
        }
    except (OSError, tarfile.TarError, EOFError, ValueError):
        return {"status": "CORRUPT", "name": archive.name}

def latest_archive(directory: str):
    paths = sorted(Path(directory).glob("locked-features-*.tar.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
    return paths[0] if paths else None

def main():
    directory = os.environ.get("KENBOT_BACKUP_DIR", "backups/locked")
    candidate = latest_archive(directory)
    if candidate is None:
        print(json.dumps({"status": "MISSING", "directory": directory}))
        return 2
    result = inspect_archive(str(candidate))
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "VERIFIED" else 1

if __name__ == "__main__":
    raise SystemExit(main())
