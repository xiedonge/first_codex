#!/usr/bin/env python3
import argparse
import csv
import datetime
import math
import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests

LIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://quote.eastmoney.com/",
}

DEFAULT_FIELDS1 = "f1,f2,f3,f4,f5,f6"
DEFAULT_FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
FIELDS2_NAME_MAP = {
    "f51": "date",
    "f52": "open",
    "f53": "close",
    "f54": "high",
    "f55": "low",
    "f56": "volume",
    "f57": "amount",
    "f58": "amplitude",
    "f59": "pct_chg",
    "f60": "chg",
    "f61": "turnover",
}

DEFAULT_KLT = 101
DEFAULT_FQT = 1
DEFAULT_BEG = 0
DEFAULT_END = 20500101
DEFAULT_LIMIT = 2000
DEFAULT_SLEEP = 0.25
DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.6
DEFAULT_UT = "fa5fd1943c7b386f172d6893dbfba10b"

LIST_FIELDS = "f12,f14,f13"
LIST_FS = "m:1+t:23"


def parse_fields(raw: str) -> List[str]:
    return [field.strip() for field in raw.split(",") if field.strip()]


def build_kline_field_names(fields2: List[str]) -> List[str]:
    names = []
    for field in fields2:
        names.append(FIELDS2_NAME_MAP.get(field, field))
    return names


def http_get_json(
    session: requests.Session,
    url: str,
    params: Dict[str, object],
    retries: int,
    backoff: float,
    timeout: int,
) -> Optional[Dict[str, object]]:
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                print(f"request failed: {url} ({exc})", file=sys.stderr)
                return None
            time.sleep(backoff * (2**attempt))
    return None


def fetch_star_list(
    session: requests.Session,
    page_size: int,
    sleep_s: float,
    max_stocks: Optional[int],
    retries: int,
    backoff: float,
    timeout: int,
    ut: Optional[str],
) -> List[Dict[str, str]]:
    params = {
        "pn": 1,
        "pz": page_size,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fs": LIST_FS,
        "fields": LIST_FIELDS,
    }
    if ut:
        params["ut"] = ut

    payload = http_get_json(session, LIST_URL, params, retries, backoff, timeout)
    if not payload or not isinstance(payload, dict):
        raise RuntimeError("failed to fetch stock list")

    data = payload.get("data") or {}
    diff = data.get("diff") or []
    total = data.get("total") or len(diff)
    pages = max(1, int(math.ceil(total / page_size)))

    items: List[Dict[str, str]] = []

    def append_items(rows: Iterable[Dict[str, object]]) -> bool:
        for row in rows:
            code = str(row.get("f12", "")).strip()
            name = str(row.get("f14", "")).strip()
            market = str(row.get("f13", "1")).strip() or "1"
            if not code:
                continue
            items.append(
                {
                    "code": code,
                    "name": name,
                    "market": market,
                    "secid": f"{market}.{code}",
                }
            )
            if max_stocks and len(items) >= max_stocks:
                return True
        return False

    append_items(diff)

    for page in range(2, pages + 1):
        if max_stocks and len(items) >= max_stocks:
            break
        params["pn"] = page
        payload = http_get_json(
            session, LIST_URL, params, retries, backoff, timeout
        )
        if not payload:
            continue
        data = payload.get("data") or {}
        diff = data.get("diff") or []
        stop = append_items(diff)
        if stop:
            break
        time.sleep(sleep_s)

    return items


def read_csv_header(path: str) -> Optional[List[str]]:
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader, None)

def parse_date_int(value: str) -> Optional[int]:
    cleaned = value.strip().replace("-", "").replace("/", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        return None
    return int(cleaned)


def compute_recent_beg(recent_days: Optional[int]) -> Optional[int]:
    if not recent_days or recent_days <= 0:
        return None
    start = datetime.date.today() - datetime.timedelta(days=recent_days - 1)
    return int(start.strftime("%Y%m%d"))


def read_existing_dates(path: str, date_field: str) -> Tuple[set, Optional[int]]:
    if not os.path.exists(path):
        return set(), None
    dates = set()
    latest = None
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            value = row.get(date_field)
            if value:
                dates.add(value)
                parsed = parse_date_int(value)
                if parsed is not None:
                    if latest is None or parsed > latest:
                        latest = parsed
    return dates, latest


def write_csv_rows(
    path: str,
    header: List[str],
    rows: List[Dict[str, str]],
    overwrite: bool,
) -> None:
    if not rows:
        return
    file_exists = os.path.exists(path)
    mode = "w" if overwrite or not file_exists else "a"
    with open(path, mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if overwrite or not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in header})


def fetch_kline_data(
    session: requests.Session,
    secid: str,
    fields1: str,
    fields2: str,
    klt: int,
    fqt: int,
    beg: int,
    end: int,
    limit: int,
    retries: int,
    backoff: float,
    timeout: int,
    ut: Optional[str],
) -> Optional[Dict[str, object]]:
    params = {
        "secid": secid,
        "fields1": fields1,
        "fields2": fields2,
        "klt": klt,
        "fqt": fqt,
        "beg": beg,
        "end": end,
        "lmt": limit,
    }
    if ut:
        params["ut"] = ut
    payload = http_get_json(session, KLINE_URL, params, retries, backoff, timeout)
    if not payload or not isinstance(payload, dict):
        return None
    return payload.get("data")


def build_rows(
    data: Dict[str, object],
    base_meta: Dict[str, str],
    kline_fields: List[str],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    kline_values = data.get("klines") or []
    meta_keys = [key for key in data.keys() if key != "klines"]

    for raw in kline_values:
        if not isinstance(raw, str):
            continue
        values = raw.split(",")
        row: Dict[str, str] = dict(base_meta)
        for idx, field in enumerate(kline_fields):
            row[field] = values[idx] if idx < len(values) else ""
        for key in meta_keys:
            if key in base_meta:
                continue
            row[f"meta_{key}"] = str(data.get(key, ""))
        rows.append(row)
    return rows


def write_stock_list_csv(path: str, items: List[Dict[str, str]]) -> None:
    header = ["code", "name", "market", "secid", "updated_at"]
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for item in items:
        row = dict(item)
        row["updated_at"] = now
        rows.append(row)
    write_csv_rows(path, header, rows, overwrite=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch STAR Market daily kline data from Eastmoney."
    )
    parser.add_argument("--out-dir", default="data", help="Output directory.")
    parser.add_argument("--codes", help="Comma-separated stock codes to fetch.")
    parser.add_argument(
        "--max-stocks", type=int, default=None, help="Limit number of stocks."
    )
    parser.add_argument("--klt", type=int, default=DEFAULT_KLT)
    parser.add_argument("--fqt", type=int, default=DEFAULT_FQT)
    parser.add_argument("--beg", type=int, default=DEFAULT_BEG)
    parser.add_argument("--end", type=int, default=DEFAULT_END)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--fields1", default=DEFAULT_FIELDS1)
    parser.add_argument("--fields2", default=DEFAULT_FIELDS2)
    parser.add_argument(
        "--recent-days",
        type=int,
        default=None,
        help="Limit to recent N days by capping the begin date.",
    )
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

    if args.codes:
        codes = [code.strip() for code in args.codes.split(",") if code.strip()]
        items = [
            {"code": code, "name": "", "market": "1", "secid": f"1.{code}"}
            for code in codes
        ]
    else:
        items = fetch_star_list(
            session,
            page_size=200,
            sleep_s=args.sleep,
            max_stocks=args.max_stocks,
            retries=args.retries,
            backoff=args.backoff,
            timeout=args.timeout,
            ut=args.ut,
        )
        stock_list_path = os.path.join(out_dir, "stock_basic.csv")
        write_stock_list_csv(stock_list_path, items)

    fields2_list = parse_fields(args.fields2)
    kline_fields = build_kline_field_names(fields2_list)
    date_field = "date" if "date" in kline_fields else None
    recent_beg = compute_recent_beg(args.recent_days)

    for item in items:
        code = item["code"]
        secid = item["secid"]
        output_path = os.path.join(daily_dir, f"{code}.csv")
        existing_dates = set()
        latest_date = None
        effective_beg = args.beg
        if date_field and not args.overwrite:
            existing_dates, latest_date = read_existing_dates(
                output_path, date_field
            )
            if latest_date is not None:
                effective_beg = max(effective_beg, latest_date)
        if recent_beg is not None:
            effective_beg = max(effective_beg, recent_beg)
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

        existing_header = read_csv_header(output_path)
        computed_header = list(base_meta.keys()) + kline_fields
        meta_keys = [key for key in data.keys() if key != "klines"]
        for key in meta_keys:
            if key in base_meta:
                continue
            computed_header.append(f"meta_{key}")

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
