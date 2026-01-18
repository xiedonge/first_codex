#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from typing import Dict, List, Optional, Tuple


def parse_date_int(value: str) -> Optional[int]:
    cleaned = value.strip().replace("-", "").replace("/", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        return None
    return int(cleaned)


def parse_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_ma(values: List[float], window: int) -> List[Optional[float]]:
    count = len(values)
    ma = [None] * count
    if count < window:
        return ma
    window_sum = sum(values[:window])
    ma[window - 1] = window_sum / window
    for idx in range(window, count):
        window_sum += values[idx] - values[idx - window]
        ma[idx] = window_sum / window
    return ma


def load_series(path: str) -> Optional[Tuple[str, str, List[Tuple[int, str, float]]]]:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return None
        fields = reader.fieldnames
        date_field = "date" if "date" in fields else "f51" if "f51" in fields else None
        close_field = "close" if "close" in fields else "f53" if "f53" in fields else None
        if not date_field or not close_field:
            return None

        code = ""
        name = ""
        rows: List[Tuple[int, str, float]] = []
        for row in reader:
            date_raw = (row.get(date_field) or "").strip()
            date_int = parse_date_int(date_raw)
            close_val = parse_float(row.get(close_field, ""))
            if date_int is None or close_val is None:
                continue
            if not code:
                code = (row.get("code") or "").strip()
                name = (row.get("name") or "").strip()
            rows.append((date_int, date_raw, close_val))

    if not rows:
        return None
    rows.sort(key=lambda item: item[0])
    return code, name, rows


def find_signal(
    rows: List[Tuple[int, str, float]],
    window_days: int,
    ma_window: int,
) -> Optional[Dict[str, str]]:
    if len(rows) < ma_window:
        return None
    closes = [row[2] for row in rows]
    ma_values = compute_ma(closes, ma_window)
    count = len(rows)
    window_start = max(ma_window - 1, count - window_days)

    down_idx = None
    up_idx = None
    for idx in range(window_start, count):
        if idx == 0 or ma_values[idx] is None or ma_values[idx - 1] is None:
            continue
        prev_close = closes[idx - 1]
        prev_ma = ma_values[idx - 1]
        close = closes[idx]
        cur_ma = ma_values[idx]
        if down_idx is None:
            if prev_close >= prev_ma and close < cur_ma:
                down_idx = idx
        else:
            if prev_close < prev_ma and close >= cur_ma:
                up_idx = idx
                break

    if down_idx is None or up_idx is None:
        return None

    last_idx = count - 1
    return {
        "down_date": rows[down_idx][1],
        "up_date": rows[up_idx][1],
        "last_date": rows[last_idx][1],
        "last_close": f"{closes[last_idx]:.4f}",
        "last_ma": f"{ma_values[last_idx]:.4f}",
    }


def write_results(path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    header = [
        "code",
        "name",
        "down_date",
        "up_date",
        "last_date",
        "last_close",
        "last_ma120",
        "window_days",
        "ma_window",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in header})


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Find stocks that crossed below MA120 and returned above within "
            "the last N trading days."
        )
    )
    parser.add_argument("--data-dir", default="data/daily")
    parser.add_argument("--out", default="data/signals_ma120.csv")
    parser.add_argument("--window", type=int, default=20)
    parser.add_argument("--ma", type=int, default=120)
    args = parser.parse_args()

    if args.window <= 0 or args.ma <= 1:
        print("invalid window or ma setting", file=sys.stderr)
        return 2

    results: List[Dict[str, str]] = []
    if not os.path.isdir(args.data_dir):
        print(f"data directory not found: {args.data_dir}", file=sys.stderr)
        return 1

    for name in os.listdir(args.data_dir):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(args.data_dir, name)
        loaded = load_series(path)
        if not loaded:
            continue
        code, stock_name, rows = loaded
        signal = find_signal(rows, args.window, args.ma)
        if not signal:
            continue
        result = {
            "code": code or os.path.splitext(name)[0],
            "name": stock_name,
            "down_date": signal["down_date"],
            "up_date": signal["up_date"],
            "last_date": signal["last_date"],
            "last_close": signal["last_close"],
            "last_ma120": signal["last_ma"],
            "window_days": str(args.window),
            "ma_window": str(args.ma),
        }
        results.append(result)

    write_results(args.out, results)
    print(f"matched {len(results)} symbols")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
