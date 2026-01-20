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


def load_series(
    path: str, price_field: str
) -> Optional[Tuple[str, str, List[Tuple[int, str, float, Optional[float], Optional[float]]]]]:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return None
        fields = reader.fieldnames
        date_field = "date" if "date" in fields else "f51" if "f51" in fields else None
        high_field = "high" if "high" in fields else "f54" if "f54" in fields else None
        close_field = "close" if "close" in fields else "f53" if "f53" in fields else None
        if not date_field:
            return None
        if price_field == "high":
            if not high_field:
                return None
            price_col = high_field
        else:
            if not close_field:
                return None
            price_col = close_field

        code = ""
        name = ""
        rows: List[Tuple[int, str, float, Optional[float], Optional[float]]] = []
        for row in reader:
            date_raw = (row.get(date_field) or "").strip()
            date_int = parse_date_int(date_raw)
            price_val = parse_float(row.get(price_col, ""))
            if date_int is None or price_val is None:
                continue
            if not code:
                code = (row.get("code") or "").strip()
                name = (row.get("name") or "").strip()
            high_val = parse_float(row.get(high_field, "")) if high_field else None
            close_val = parse_float(row.get(close_field, "")) if close_field else None
            rows.append((date_int, date_raw, price_val, high_val, close_val))

    if not rows:
        return None
    rows.sort(key=lambda item: item[0])
    return code, name, rows


def find_new_high(
    rows: List[Tuple[int, str, float, Optional[float], Optional[float]]],
    window_days: int,
    include_equal: bool,
) -> Optional[Dict[str, str]]:
    if len(rows) < window_days or window_days < 2:
        return None
    window_rows = rows[-window_days:]
    prices = [row[2] for row in window_rows]
    last_price = prices[-1]
    prior_max = max(prices[:-1])
    if include_equal:
        if last_price < prior_max:
            return None
    else:
        if last_price <= prior_max:
            return None
    window_max = max(prices)
    last_high = window_rows[-1][3]
    last_close = window_rows[-1][4]
    return {
        "window_start": window_rows[0][1],
        "last_date": window_rows[-1][1],
        "last_price": f"{last_price:.4f}",
        "prior_max": f"{prior_max:.4f}",
        "window_max": f"{window_max:.4f}",
        "last_high": f"{last_high:.4f}" if last_high is not None else "",
        "last_close": f"{last_close:.4f}" if last_close is not None else "",
    }


def write_results(path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    header = [
        "code",
        "name",
        "price_field",
        "window_days",
        "include_equal",
        "window_start",
        "last_date",
        "last_price",
        "prior_max",
        "window_max",
        "last_high",
        "last_close",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in header})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Find stocks making a 20-day new high on the latest bar."
    )
    parser.add_argument("--data-dir", default="data/daily")
    parser.add_argument("--out", default="data/signals_new_high_20d.csv")
    parser.add_argument("--window", type=int, default=20)
    parser.add_argument(
        "--price-field",
        choices=["high", "close"],
        default="high",
        help="Price field used to determine new highs.",
    )
    parser.add_argument(
        "--include-equal",
        action="store_true",
        help="Treat equal highs as new highs.",
    )
    args = parser.parse_args()

    if args.window <= 1:
        print("window must be >= 2", file=sys.stderr)
        return 2
    if not os.path.isdir(args.data_dir):
        print(f"data directory not found: {args.data_dir}", file=sys.stderr)
        return 1

    results: List[Dict[str, str]] = []
    for name in os.listdir(args.data_dir):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(args.data_dir, name)
        loaded = load_series(path, args.price_field)
        if not loaded:
            continue
        code, stock_name, rows = loaded
        signal = find_new_high(rows, args.window, args.include_equal)
        if not signal:
            continue
        results.append(
            {
                "code": code or os.path.splitext(name)[0],
                "name": stock_name,
                "price_field": args.price_field,
                "window_days": str(args.window),
                "include_equal": str(args.include_equal).lower(),
                **signal,
            }
        )

    write_results(args.out, results)
    print(f"matched {len(results)} symbols")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
