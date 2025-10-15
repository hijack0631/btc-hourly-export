#!/usr/bin/env python3
"""
get_btc_hourly_coingecko.py

Fetch hourly BTC prices (USD) for the last N months from CoinGecko using /coins/bitcoin/market_chart/range.
Important: DO NOT send 'interval=hourly' (Enterprise-only). Use <=90-day chunks and CoinGecko will return hourly granularity.
Outputs CSV: time_iso_utc, time_unix_ms, price_usd
"""

import requests
import csv
import time
from datetime import datetime, timezone, timedelta
import argparse
import sys

COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/solana/market_chart/range"
MAX_DAYS_PER_REQUEST = 90  # CoinGecko provides hourly granularity for <=90 days windows

def to_unix(dt: datetime):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def chunks(start_dt: datetime, end_dt: datetime, days_chunk=MAX_DAYS_PER_REQUEST):
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(days=days_chunk))
        yield cur, nxt
        cur = nxt

def fetch_range(from_unix, to_unix, retries=6):
    params = {
        "vs_currency": "usd",
        "from": str(from_unix),
        "to": str(to_unix),
        # DO NOT include "interval" here — free plan will error if you set interval=hourly
    }
    headers = {"Accept": "application/json", "User-Agent": "cg-fetcher/1.0 (+https://your-repo.example)"}
    backoff_base = 2
    for attempt in range(retries):
        try:
            r = requests.get(COINGECKO_RANGE_URL, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            # Helpful debug output for non-200
            print(f"[WARN] HTTP {r.status_code} from CoinGecko: {r.text[:800]}", file=sys.stderr)
            if r.status_code in (429, 502, 503, 504):
                sleep = backoff_base ** attempt
                print(f"[INFO] Backoff {sleep}s (attempt {attempt+1})", file=sys.stderr)
                time.sleep(sleep)
                continue
            # If CoinGecko returns an API error (like interval misuse) raise to let caller handle/abort
            r.raise_for_status()
        except requests.RequestException as e:
            sleep = backoff_base ** attempt
            print(f"[WARN] Request error: {e} - sleeping {sleep}s (attempt {attempt+1})", file=sys.stderr)
            time.sleep(sleep)
    raise RuntimeError("Failed to fetch data from CoinGecko after retries")

def main(months=10, out="btcusdt_1h_10months.csv", delay_between=0.25):
    # compute start/end (end = now aligned to hour)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # subtract months (approx by month arithmetic)
    year = now.year
    month = now.month - months
    while month <= 0:
        month += 12
        year -= 1
    # choose 1st of month to keep it safe (you can change if you prefer same day)
    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = now

    print(f"[INFO] Fetching BTC hourly (USD) from {start_dt.isoformat()} to {end_dt.isoformat()} (UTC)")

    rows = []
    for s, e in chunks(start_dt, end_dt, days_chunk=MAX_DAYS_PER_REQUEST):
        from_unix = to_unix(s)
        to_unix_ts = to_unix(e)
        print(f"[INFO] Request chunk: {s.date()} -> {e.date()}  (from={from_unix} to={to_unix_ts})")
        data = fetch_range(from_unix, to_unix_ts)
        if not data:
            print("[ERROR] Empty response for chunk — aborting.", file=sys.stderr)
            raise RuntimeError("Empty response from CoinGecko")
        prices = data.get("prices", [])
        if not prices:
            print("[WARN] No prices returned for chunk; response keys: " + ", ".join(data.keys()), file=sys.stderr)
        for p in prices:
            ts_ms = int(p[0])
            price = float(p[1])
            iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
            rows.append((iso, ts_ms, price))
        time.sleep(delay_between)

    # dedupe by timestamp and sort
    unique = {}
    for iso, ms, price in rows:
        unique[ms] = (iso, ms, price)
    sorted_rows = [unique[k] for k in sorted(unique.keys())]

    # write CSV
    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time_iso_utc", "time_unix_ms", "price_usd"])
        for iso, ms, price in sorted_rows:
            writer.writerow([iso, ms, f"{price:.8f}"])

    print(f"[INFO] Wrote {len(sorted_rows)} rows -> {out}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=10, help="How many months back to fetch")
    parser.add_argument("--out", type=str, default="btcusdt_1h_10months.csv", help="Output CSV filename")
    args = parser.parse_args()
    main(months=args.months, out=args.out)