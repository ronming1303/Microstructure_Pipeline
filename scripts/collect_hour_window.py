from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


BASE_URL = "https://api.exchange.coinbase.com"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def fetch_order_book(product_id: str, level: int = 2, timeout: float = 10.0) -> dict[str, Any]:
    url = f"{BASE_URL}/products/{quote(product_id)}/book?level={level}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "microstructure-pipeline/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_levels(raw_levels: list[Any], depth: int) -> list[dict[str, Any]]:
    levels: list[dict[str, Any]] = []
    for raw_level in raw_levels[:depth]:
        if not isinstance(raw_level, (list, tuple)) or len(raw_level) < 2:
            continue
        price = float(raw_level[0])
        size = float(raw_level[1])
        num_orders = int(raw_level[2]) if len(raw_level) > 2 else None
        levels.append({"price": price, "size": size, "num_orders": num_orders})
    return levels


def compute_snapshot_metrics(bids: list[dict[str, Any]], asks: list[dict[str, Any]]) -> dict[str, Any]:
    bid_volume = sum(level["size"] for level in bids)
    ask_volume = sum(level["size"] for level in asks)
    denominator = bid_volume + ask_volume
    imbalance = (bid_volume - ask_volume) / denominator if denominator else 0.0

    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    mid_price = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
    spread_bps = (spread / mid_price * 10_000) if spread is not None and mid_price not in (None, 0) else None

    return {
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
        "imbalance": imbalance,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid_price,
        "spread": spread,
        "spread_bps": spread_bps,
    }


def write_csv_row(path: Path, fieldnames: list[str], row: dict[str, Any]) -> None:
    ensure_parent(path)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def as_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.12f}".rstrip("0").rstrip(".")
    return value


MINUTE_FIELDS = [
    "window_id",
    "sample_index",
    "planned_at_utc",
    "collected_at_utc",
    "product_id",
    "depth",
    "status",
    "error",
    "sequence",
    "snapshot_time_utc",
    "bid_volume",
    "ask_volume",
    "imbalance",
    "best_bid",
    "best_ask",
    "mid_price",
    "spread",
    "spread_bps",
    "bids_json",
    "asks_json",
]


HOURLY_FIELDS = [
    "window_id",
    "window_start_utc",
    "window_end_utc",
    "product_id",
    "depth",
    "target_samples",
    "successful_samples",
    "failed_samples",
    "imbalance_mean",
    "imbalance_median",
    "imbalance_min",
    "imbalance_max",
    "bid_volume_mean",
    "ask_volume_mean",
    "notes",
]


def product_slug(product_id: str) -> str:
    return product_id.lower().replace("-", "_")


def minute_shard_path(minute_dir: Path, product_id: str, depth: int, collected_at: datetime) -> Path:
    date_part = collected_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
    filename = f"{date_part}_{product_slug(product_id)}_d{depth}.csv"
    return minute_dir / filename


def hourly_shard_path(hourly_dir: Path, product_id: str, depth: int, window_start: datetime) -> Path:
    date_part = window_start.astimezone(timezone.utc).strftime("%Y-%m-%d")
    filename = f"{date_part}_{product_slug(product_id)}_hourly_d{depth}.csv"
    return hourly_dir / filename


def build_window_id(start: datetime, product_id: str, depth: int) -> str:
    return f"{product_slug(product_id)}_d{depth}_{start.strftime('%Y%m%dT%H%M%SZ')}"


def collect_window(
    product_id: str,
    depth: int,
    minutes: int,
    interval_seconds: int,
    minute_dir: Path,
    hourly_dir: Path,
    state_file: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    window_start = utc_now()
    window_id = build_window_id(window_start, product_id, depth)
    rows: list[dict[str, Any]] = []
    minute_files_written: set[str] = set()
    hourly_files_written: set[str] = set()

    for sample_index in range(minutes):
        planned_at = window_start + timedelta(seconds=sample_index * interval_seconds)
        sleep_seconds = (planned_at - utc_now()).total_seconds()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        collected_at = utc_now()
        row: dict[str, Any] = {
            "window_id": window_id,
            "sample_index": sample_index,
            "planned_at_utc": isoformat_z(planned_at),
            "collected_at_utc": isoformat_z(collected_at),
            "product_id": product_id,
            "depth": depth,
            "status": "ok",
            "error": "",
            "sequence": "",
            "snapshot_time_utc": "",
            "bid_volume": "",
            "ask_volume": "",
            "imbalance": "",
            "best_bid": "",
            "best_ask": "",
            "mid_price": "",
            "spread": "",
            "spread_bps": "",
            "bids_json": "",
            "asks_json": "",
        }

        try:
            snapshot = fetch_order_book(product_id=product_id, level=2, timeout=timeout_seconds)
            bids = parse_levels(snapshot.get("bids", []), depth)
            asks = parse_levels(snapshot.get("asks", []), depth)
            metrics = compute_snapshot_metrics(bids, asks)
            row.update(
                {
                    "sequence": snapshot.get("sequence", ""),
                    "snapshot_time_utc": snapshot.get("time", ""),
                    "bid_volume": as_csv_value(metrics["bid_volume"]),
                    "ask_volume": as_csv_value(metrics["ask_volume"]),
                    "imbalance": as_csv_value(metrics["imbalance"]),
                    "best_bid": as_csv_value(metrics["best_bid"]),
                    "best_ask": as_csv_value(metrics["best_ask"]),
                    "mid_price": as_csv_value(metrics["mid_price"]),
                    "spread": as_csv_value(metrics["spread"]),
                    "spread_bps": as_csv_value(metrics["spread_bps"]),
                    "bids_json": json.dumps(bids, separators=(",", ":")),
                    "asks_json": json.dumps(asks, separators=(",", ":")),
                }
            )
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            row["status"] = "error"
            row["error"] = str(exc)

        rows.append(row)
        minute_csv = minute_shard_path(minute_dir=minute_dir, product_id=product_id, depth=depth, collected_at=collected_at)
        minute_files_written.add(str(minute_csv.as_posix()))
        write_csv_row(minute_csv, MINUTE_FIELDS, {key: as_csv_value(value) for key, value in row.items()})

    window_end = utc_now()
    successful_rows = [row for row in rows if row["status"] == "ok"]
    imbalance_values = [float(row["imbalance"]) for row in successful_rows if row["imbalance"] != ""]
    bid_volume_values = [float(row["bid_volume"]) for row in successful_rows if row["bid_volume"] != ""]
    ask_volume_values = [float(row["ask_volume"]) for row in successful_rows if row["ask_volume"] != ""]

    summary = {
        "window_id": window_id,
        "window_start_utc": isoformat_z(window_start),
        "window_end_utc": isoformat_z(window_end),
        "product_id": product_id,
        "depth": depth,
        "target_samples": minutes,
        "successful_samples": len(successful_rows),
        "failed_samples": len(rows) - len(successful_rows),
        "imbalance_mean": as_csv_value(statistics.mean(imbalance_values)) if imbalance_values else "",
        "imbalance_median": as_csv_value(statistics.median(imbalance_values)) if imbalance_values else "",
        "imbalance_min": as_csv_value(min(imbalance_values)) if imbalance_values else "",
        "imbalance_max": as_csv_value(max(imbalance_values)) if imbalance_values else "",
        "bid_volume_mean": as_csv_value(statistics.mean(bid_volume_values)) if bid_volume_values else "",
        "ask_volume_mean": as_csv_value(statistics.mean(ask_volume_values)) if ask_volume_values else "",
        "notes": "hourly window sampler; minute rows are collected live during the run",
    }
    hourly_csv = hourly_shard_path(hourly_dir=hourly_dir, product_id=product_id, depth=depth, window_start=window_start)
    hourly_files_written.add(str(hourly_csv.as_posix()))
    write_csv_row(hourly_csv, HOURLY_FIELDS, summary)

    ensure_parent(state_file)
    state_file.write_text(
        json.dumps(
            {
                "last_window_id": window_id,
                "last_window_start_utc": summary["window_start_utc"],
                "last_window_end_utc": summary["window_end_utc"],
                "last_product_id": product_id,
                "last_depth": depth,
                "last_successful_samples": len(successful_rows),
                "last_failed_samples": len(rows) - len(successful_rows),
                "last_minute_files_written": sorted(minute_files_written),
                "last_hourly_files_written": sorted(hourly_files_written),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect minute-level Coinbase order-book imbalance within an hourly run.")
    parser.add_argument("--product", default="BTC-USD", help="Coinbase product id, for example BTC-USD")
    parser.add_argument("--depth", type=int, default=10, help="How many levels to include from each side")
    parser.add_argument("--minutes", type=int, default=60, help="How many minute samples to collect in one run")
    parser.add_argument("--interval-seconds", type=int, default=60, help="Sampling interval in seconds")
    parser.add_argument("--timeout-seconds", type=float, default=10.0, help="HTTP timeout for the order-book request")
    parser.add_argument("--minute-dir", type=Path, default=Path("data/minute"), help="Directory for daily minute shard files")
    parser.add_argument("--hourly-dir", type=Path, default=Path("data/hourly"), help="Directory for daily hourly shard files")
    parser.add_argument("--state-file", type=Path, default=Path("state/collector_state.json"), help="Path for the JSON state file")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    summary = collect_window(
        product_id=args.product,
        depth=args.depth,
        minutes=args.minutes,
        interval_seconds=args.interval_seconds,
        minute_dir=args.minute_dir,
        hourly_dir=args.hourly_dir,
        state_file=args.state_file,
        timeout_seconds=args.timeout_seconds,
    )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())