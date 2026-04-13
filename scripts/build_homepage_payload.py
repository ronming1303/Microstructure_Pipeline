from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def parse_float(value: str) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def read_minute_rows_from_dir(minute_dir: Path) -> list[dict[str, str]]:
    if not minute_dir.exists():
        return []

    rows: list[dict[str, str]] = []
    for shard_path in sorted(minute_dir.glob("*.csv")):
        rows.extend(read_csv_rows(shard_path))
    return rows


def read_hourly_rows_from_dir(hourly_dir: Path) -> list[dict[str, str]]:
    if not hourly_dir.exists():
        return []

    rows: list[dict[str, str]] = []
    for shard_path in sorted(hourly_dir.glob("*_hourly_d*.csv")):
        rows.extend(read_csv_rows(shard_path))
    return rows


def write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def build_latest_payload(minute_rows: list[dict[str, str]], hourly_rows: list[dict[str, str]]) -> dict[str, Any]:
    latest_minute = None
    for row in minute_rows:
        timestamp = row.get("collected_at_utc", "")
        if latest_minute is None:
            latest_minute = row
            continue
        if timestamp > latest_minute.get("collected_at_utc", ""):
            latest_minute = row

    latest_hourly = None
    for row in hourly_rows:
        timestamp = row.get("window_end_utc", "")
        if latest_hourly is None:
            latest_hourly = row
            continue
        if timestamp > latest_hourly.get("window_end_utc", ""):
            latest_hourly = row

    return {
        "latest_minute": {
            "collected_at_utc": latest_minute.get("collected_at_utc") if latest_minute else None,
            "product_id": latest_minute.get("product_id") if latest_minute else None,
            "depth": int(latest_minute["depth"]) if latest_minute and latest_minute.get("depth") else None,
            "imbalance": parse_float(latest_minute.get("imbalance", "")) if latest_minute else None,
            "mid_price": parse_float(latest_minute.get("mid_price", "")) if latest_minute else None,
            "spread_bps": parse_float(latest_minute.get("spread_bps", "")) if latest_minute else None,
            "status": latest_minute.get("status") if latest_minute else None,
        },
        "latest_hourly": {
            "window_start_utc": latest_hourly.get("window_start_utc") if latest_hourly else None,
            "window_end_utc": latest_hourly.get("window_end_utc") if latest_hourly else None,
            "product_id": latest_hourly.get("product_id") if latest_hourly else None,
            "depth": int(latest_hourly["depth"]) if latest_hourly and latest_hourly.get("depth") else None,
            "successful_samples": int(latest_hourly["successful_samples"]) if latest_hourly and latest_hourly.get("successful_samples") else None,
            "failed_samples": int(latest_hourly["failed_samples"]) if latest_hourly and latest_hourly.get("failed_samples") else None,
            "imbalance_mean": parse_float(latest_hourly.get("imbalance_mean", "")) if latest_hourly else None,
            "imbalance_median": parse_float(latest_hourly.get("imbalance_median", "")) if latest_hourly else None,
            "imbalance_min": parse_float(latest_hourly.get("imbalance_min", "")) if latest_hourly else None,
            "imbalance_max": parse_float(latest_hourly.get("imbalance_max", "")) if latest_hourly else None,
        },
    }


def build_hourly_tail_payload(hourly_rows: list[dict[str, str]], count: int) -> dict[str, Any]:
    sorted_rows = sorted(hourly_rows, key=lambda row: row.get("window_start_utc", ""))
    tail = sorted_rows[-count:] if count > 0 else []
    points: list[dict[str, Any]] = []
    for row in tail:
        points.append(
            {
                "window_start_utc": row.get("window_start_utc"),
                "window_end_utc": row.get("window_end_utc"),
                "imbalance_mean": parse_float(row.get("imbalance_mean", "")),
                "imbalance_median": parse_float(row.get("imbalance_median", "")),
                "imbalance_min": parse_float(row.get("imbalance_min", "")),
                "imbalance_max": parse_float(row.get("imbalance_max", "")),
                "successful_samples": int(row["successful_samples"]) if row.get("successful_samples") else None,
            }
        )

    return {"points": points}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build JSON payloads for homepage integration.")
    parser.add_argument("--minute-dir", type=Path, default=Path("data/minute"))
    parser.add_argument("--hourly-dir", type=Path, default=Path("data/hourly"))
    parser.add_argument("--latest-json", type=Path, default=Path("public/data/imbalance_latest.json"))
    parser.add_argument("--hourly-tail-json", type=Path, default=Path("public/data/imbalance_hourly_tail.json"))
    parser.add_argument("--hourly-tail-count", type=int, default=168, help="How many hourly points to expose")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    minute_rows = read_minute_rows_from_dir(args.minute_dir)
    hourly_rows = read_hourly_rows_from_dir(args.hourly_dir)

    latest_payload = build_latest_payload(minute_rows, hourly_rows)
    tail_payload = build_hourly_tail_payload(hourly_rows, args.hourly_tail_count)

    write_json(args.latest_json, latest_payload)
    write_json(args.hourly_tail_json, tail_payload)
    print(f"Wrote {args.latest_json} and {args.hourly_tail_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())