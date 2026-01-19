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


def check_ma_signal(
    rows: List[Tuple[int, str, float]],
    closes: List[float],
    ma_values: List[Optional[float]],
    window_days: int,
    band: float,
    max_outside: int,
    min_up_days: int,
    min_rise_pct: float,
) -> Optional[Dict[str, str]]:
    total = len(rows)
    if total < window_days:
        return None
    start_idx = total - window_days
    first_ma_idx = 0
    for idx, value in enumerate(ma_values):
        if value is not None:
            first_ma_idx = idx
            break
    if start_idx < first_ma_idx:
        return None

    start_ma = ma_values[start_idx]
    end_ma = ma_values[total - 1]
    if start_ma is None or end_ma is None or start_ma <= 0:
        return None

    outside = 0
    for idx in range(start_idx, total):
        ma = ma_values[idx]
        close = closes[idx]
        if ma is None or ma <= 0:
            return None
        if abs(close - ma) / ma > band:
            outside += 1
            if outside > max_outside:
                return None

    up_days = 0
    for idx in range(start_idx + 1, total):
        prev = ma_values[idx - 1]
        cur = ma_values[idx]
        if prev is None or cur is None:
            return None
        if cur >= prev:
            up_days += 1
    if up_days < min_up_days:
        return None

    rise_pct = (end_ma - start_ma) / start_ma
    if rise_pct < min_rise_pct:
        return None

    return {
        "start_date": rows[start_idx][1],
        "end_date": rows[total - 1][1],
        "last_close": f"{closes[total - 1]:.4f}",
        "last_ma": f"{end_ma:.4f}",
        "up_days": str(up_days),
        "rise_pct": f"{rise_pct:.6f}",
    }


def write_results(path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    header = [
        "code",
        "name",
        "ma_window",
        "window_days",
        "band",
        "max_outside",
        "min_up_days",
        "min_rise_pct",
        "up_days",
        "rise_pct",
        "start_date",
        "end_date",
        "last_close",
        "last_ma",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in header})


def parse_ma_list(value: str) -> List[int]:
    result = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            num = int(part)
        except ValueError:
            continue
        if num > 1:
            result.append(num)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Find stocks that stay near MA20 or MA30 and trend upward in the "
            "last N trading days."
        )
    )
    parser.add_argument("--data-dir", default="data/daily")
    parser.add_argument("--out", default="data/signals_ma20_ma30.csv")
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--ma-list", default="20,30")
    parser.add_argument("--band", type=float, default=0.02)
    parser.add_argument("--max-outside", type=int, default=2)
    parser.add_argument("--min-up-days", type=int, default=None)
    parser.add_argument("--min-rise-pct", type=float, default=0.0)
    args = parser.parse_args()

    if args.window <= 0:
        print("invalid window setting", file=sys.stderr)
        return 2

    ma_list = parse_ma_list(args.ma_list)
    if not ma_list:
        print("invalid ma list", file=sys.stderr)
        return 2

    min_up_days = args.min_up_days
    if min_up_days is None:
        min_up_days = max(1, args.window - 4)

    if not os.path.isdir(args.data_dir):
        print(f"data directory not found: {args.data_dir}", file=sys.stderr)
        return 1

    results: List[Dict[str, str]] = []
    for name in os.listdir(args.data_dir):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(args.data_dir, name)
        loaded = load_series(path)
        if not loaded:
            continue
        code, stock_name, rows = loaded
        closes = [row[2] for row in rows]

        for ma_window in ma_list:
            ma_values = compute_ma(closes, ma_window)
            signal = check_ma_signal(
                rows,
                closes,
                ma_values,
                args.window,
                args.band,
                args.max_outside,
                min_up_days,
                args.min_rise_pct,
            )
            if not signal:
                continue
            result = {
                "code": code or os.path.splitext(name)[0],
                "name": stock_name,
                "ma_window": str(ma_window),
                "window_days": str(args.window),
                "band": f"{args.band:.4f}",
                "max_outside": str(args.max_outside),
                "min_up_days": str(min_up_days),
                "min_rise_pct": f"{args.min_rise_pct:.6f}",
                "up_days": signal["up_days"],
                "rise_pct": signal["rise_pct"],
                "start_date": signal["start_date"],
                "end_date": signal["end_date"],
                "last_close": signal["last_close"],
                "last_ma": signal["last_ma"],
            }
            results.append(result)
            break

    write_results(args.out, results)
    print(f"matched {len(results)} symbols")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
