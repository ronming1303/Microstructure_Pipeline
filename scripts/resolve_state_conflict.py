#!/usr/bin/env python3
"""Resolve a git merge conflict in the collector state JSON file.

The state file is a write-only status snapshot (collect_hour_window.py never
reads it back to decide behavior), so on conflict we simply keep whichever
side recorded the later collection window.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path


def read_stage(path: str, stage: int):
    result = subprocess.run(
        ["git", "show", f":{stage}:{path}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="Path to the conflicted state file")
    args = parser.parse_args()

    ours = read_stage(args.path, 2)
    theirs = read_stage(args.path, 3)

    if ours is None and theirs is None:
        print(f"No conflicted stages found for {args.path}", file=sys.stderr)
        return 1
    if ours is None:
        winner = theirs
    elif theirs is None:
        winner = ours
    else:
        winner = ours if ours.get("last_window_end_utc", "") >= theirs.get("last_window_end_utc", "") else theirs

    Path(args.path).write_text(json.dumps(winner, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
