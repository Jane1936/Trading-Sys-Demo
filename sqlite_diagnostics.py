"""Read-only SQLite diagnostics for incident evidence collection."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import db_config


def _mount_for(path: Path) -> dict[str, str]:
    """Return the most-specific Linux mount containing *path*."""
    resolved = path.resolve()
    best: tuple[int, dict[str, str]] | None = None
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return {"mount_point": "unknown", "filesystem": "unknown", "source": "unknown"}
    for line in lines:
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        fields, fs_fields = left.split(), right.split()
        if len(fields) < 5 or len(fs_fields) < 2:
            continue
        mount_point = Path(fields[4].replace("\\040", " "))
        try:
            resolved.relative_to(mount_point)
        except ValueError:
            continue
        candidate = {"mount_point": str(mount_point), "filesystem": fs_fields[0], "source": fs_fields[1]}
        specificity = len(mount_point.parts)
        if best is None or specificity > best[0]:
            best = (specificity, candidate)
    return best[1] if best else {"mount_point": "unknown", "filesystem": "unknown", "source": "unknown"}


def _file_state(path: Path) -> dict[str, object]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {"exists": False}
    return {
        "exists": True,
        "size_bytes": stat.st_size,
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "device": stat.st_dev,
        "inode": stat.st_ino,
    }


def inspect_database(db_path: str) -> dict[str, object]:
    """Inspect one database without creating it or changing its journal mode."""
    path = Path(db_path).resolve()
    usage = os.statvfs(path.parent) if path.parent.exists() else None
    report: dict[str, object] = {
        "path": str(path),
        "mount": _mount_for(path),
        "files": {
            suffix or "main": _file_state(Path(f"{path}{suffix}"))
            for suffix in ("", "-wal", "-shm", ".recovering", ".access.lock")
        },
        "disk": {"free_bytes": usage.f_bavail * usage.f_frsize, "free_inodes": usage.f_favail} if usage else None,
    }
    if not path.exists():
        report["health"] = {"status": "missing"}
        return report

    uri = f"file:{quote(str(path))}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True, timeout=5) as conn:
            quick_check = conn.execute("PRAGMA quick_check").fetchone()
            report["sqlite"] = {
                "version": sqlite3.sqlite_version,
                "journal_mode": conn.execute("PRAGMA journal_mode").fetchone()[0],
                "synchronous": conn.execute("PRAGMA synchronous").fetchone()[0],
                "page_size": conn.execute("PRAGMA page_size").fetchone()[0],
                "page_count": conn.execute("PRAGMA page_count").fetchone()[0],
                "freelist_count": conn.execute("PRAGMA freelist_count").fetchone()[0],
                "quick_check": quick_check[0] if quick_check else "no result",
            }
        report["health"] = {"status": "ok" if report["sqlite"]["quick_check"] == "ok" else "corrupt"}
    except sqlite3.Error as exc:
        report["health"] = {"status": "error", "detail": str(exc)}
    return report


def build_report(paths: list[str]) -> dict[str, object]:
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "databases": [inspect_database(path) for path in paths],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect read-only SQLite integrity, sidecar, mount, and capacity evidence.")
    parser.add_argument("paths", nargs="*", help="DB paths; defaults to all configured DBs")
    parser.add_argument("--output", help="Write JSON to this file instead of stdout")
    args = parser.parse_args()
    paths = args.paths or list(db_config.DB_LABELS.values())
    rendered = json.dumps(build_report(paths), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
