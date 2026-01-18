#!/usr/bin/env python3
import argparse
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

from kcb_daily import (
    DEFAULT_BACKOFF,
    DEFAULT_BEG,
    DEFAULT_END,
    DEFAULT_FIELDS1,
    DEFAULT_FIELDS2,
    DEFAULT_FQT,
    DEFAULT_HEADERS,
    DEFAULT_KLT,
    DEFAULT_LIMIT,
    DEFAULT_RETRIES,
    DEFAULT_SLEEP,
    DEFAULT_TIMEOUT,
    DEFAULT_UT,
    LIST_FS,
    build_kline_field_names,
    build_rows,
    fetch_kline_data,
    fetch_star_list,
    parse_fields,
    read_csv_header,
    read_existing_dates,
    write_csv_rows,
    write_stock_list_csv,
)


def build_header(
    base_meta: Dict[str, str],
    kline_fields: List[str],
    data: Dict[str, object],
) -> List[str]:
    header = list(base_meta.keys()) + kline_fields
    for key in data.keys():
        if key == "klines" or key in base_meta:
            continue
        header.append(f"meta_{key}")
    return header


def prepare_items(
    session: requests.Session,
    codes: Optional[str],
    out_dir: str,
    list_fs: str,
    max_stocks: Optional[int],
    sleep_s: float,
    retries: int,
    backoff: float,
    timeout: int,
    ut: Optional[str],
) -> List[Dict[str, str]]:
    if codes:
        values = [code.strip() for code in codes.split(",") if code.strip()]
        return [
            {"code": code, "name": "", "market": "1", "secid": f"1.{code}"}
            for code in values
        ]

    items = fetch_star_list(
        session,
        page_size=200,
        sleep_s=sleep_s,
        max_stocks=max_stocks,
        list_fs=list_fs,
        retries=retries,
        backoff=backoff,
        timeout=timeout,
        ut=ut,
    )
    stock_list_path = os.path.join(out_dir, "stock_basic.csv")
    write_stock_list_csv(stock_list_path, items)
    return items


def load_existing_state(
    path: str, date_field: Optional[str]
) -> Tuple[set, Optional[int], Optional[List[str]]]:
    header = read_csv_header(path)
    if not date_field or not header or date_field not in header:
        return set(), None, header
    dates, latest_date = read_existing_dates(path, date_field)
    return dates, latest_date, header


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Incrementally update STAR Market daily kline data."
    )
    parser.add_argument("--out-dir", default="data", help="Output directory.")
    parser.add_argument("--codes", help="Comma-separated stock codes to fetch.")
    parser.add_argument(
        "--max-stocks", type=int, default=None, help="Limit number of stocks."
    )
    parser.add_argument(
        "--list-fs",
        default=LIST_FS,
        help="Stock list filter for Eastmoney clist/get.",
    )
    parser.add_argument("--klt", type=int, default=DEFAULT_KLT)
    parser.add_argument("--fqt", type=int, default=DEFAULT_FQT)
    parser.add_argument("--beg", type=int, default=DEFAULT_BEG)
    parser.add_argument("--end", type=int, default=DEFAULT_END)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--fields1", default=DEFAULT_FIELDS1)
    parser.add_argument("--fields2", default=DEFAULT_FIELDS2)
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF)
    parser.add_argument("--ut", default=DEFAULT_UT)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing per-stock CSV files.",
    )
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    out_dir = args.out_dir
    daily_dir = os.path.join(out_dir, "daily")
    os.makedirs(daily_dir, exist_ok=True)

    items = prepare_items(
        session=session,
        codes=args.codes,
        out_dir=out_dir,
        list_fs=args.list_fs,
        max_stocks=args.max_stocks,
        sleep_s=args.sleep,
        retries=args.retries,
        backoff=args.backoff,
        timeout=args.timeout,
        ut=args.ut,
    )

    fields2_list = parse_fields(args.fields2)
    kline_fields = build_kline_field_names(fields2_list)
    date_field = "date" if "date" in kline_fields else None

    for item in items:
        code = item["code"]
        secid = item["secid"]
        output_path = os.path.join(daily_dir, f"{code}.csv")
        existing_dates = set()
        latest_date = None
        existing_header = None
        if not args.overwrite:
            existing_dates, latest_date, existing_header = load_existing_state(
                output_path, date_field
            )

        effective_beg = args.beg
        if latest_date is not None:
            effective_beg = max(effective_beg, latest_date)

        data = fetch_kline_data(
            session,
            secid=secid,
            fields1=args.fields1,
            fields2=args.fields2,
            klt=args.klt,
            fqt=args.fqt,
            beg=effective_beg,
            end=args.end,
            limit=args.limit,
            retries=args.retries,
            backoff=args.backoff,
            timeout=args.timeout,
            ut=args.ut,
        )
        if not data:
            print(f"skip {code}: empty response", file=sys.stderr)
            continue

        base_meta = {
            "code": item["code"],
            "name": item.get("name", ""),
            "market": item.get("market", ""),
            "secid": item.get("secid", ""),
        }
        rows = build_rows(data, base_meta, kline_fields)
        if not rows:
            print(f"skip {code}: no kline rows", file=sys.stderr)
            continue

        computed_header = build_header(base_meta, kline_fields, data)
        header = existing_header or computed_header
        if existing_header:
            missing = set(computed_header) - set(existing_header)
            if missing:
                print(
                    f"warning {code}: header missing {sorted(missing)}",
                    file=sys.stderr,
                )

        if date_field and not args.overwrite:
            rows = [row for row in rows if row.get(date_field) not in existing_dates]

        write_csv_rows(output_path, header, rows, overwrite=args.overwrite)
        time.sleep(args.sleep)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
