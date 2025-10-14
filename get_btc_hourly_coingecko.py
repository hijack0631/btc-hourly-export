#!/usr/bin/env python3
"""
get_btc_hourly_coingecko.py
Fetch hourly BTC price points for the last N months from CoinGecko, chunked into <=90-day ranges,
and save as CSV with ISO timestamps and price (close-equivalent).
"""

import requests
import csv
import time
from datetime import datetime, timezone, timedelta
import math
import argparse
import sys

COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
# CoinGecko: hourly interval supported for up to ~90 days per request
MAX_DAYS_PER_REQUEST = 90

def to_unix_ms(dt: datetime):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def to_unix(dt: datetime):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def fetch_range(from_unix, to_unix, retries=6, backoff_base=2):
    params = {
        "vs_currency": "usd",
        "from": str(from_unix),
        "to": str(to_unix),
        "interval": "hourly"  # request hourly; CoinGecko uses auto-granularity but this forces hourly where available
    }
    headers = {"Accept": "application/json", "User-Agent": "cg-fetcher/1.0"}
    for attempt in range(retries):
        try:
            r = requests.get(COINGECKO_RANGE_URL, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            else:
                # print truncated body for debug
                print(f"HTTP {r.status_code} from CoinGecko: {r.text[:500]}", file=sys.stderr)
                # 429: backoff and retry
                if r.status_code in (429, 502, 503, 504):
                    sleep = backoff_base ** attempt
                    print(f"Backoff {sleep}s (attempt {attempt+1})", file=sys.stderr)
                    time.sleep(sleep)
                    continue
                # Other errors: raise
                r.raise_for_status()
        except requests.RequestException as e:
            sleep = backoff_base ** attempt
            print(f"Request error: {e} - sleeping {sleep}s", file=sys.stderr)
            time.sleep(sleep)
    raise RuntimeError("Failed to fetch data from CoinGecko after retries")

def chunks(start_dt: datetime, end_dt: datetime, days_chunk=MAX_DAYS_PER_REQUEST):
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(days=days_chunk))
        yield cur, nxt
        cur = nxt

def main(months=10, out="btcusdt_1h_10months.csv", tz_out="UTC", delay_between=0.25):
    # compute start/end (end = now aligned to hour)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # subtract months (approx by subtracting months preserving day)
    # Simple approach: move back months by adjusting year/month
    year = now.year
    month = now.month - months
    while month <= 0:
        month += 12
        year -= 1
    # keep same day but clamp to 1 to avoid month-end issues
    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = now

    print(f"Fetching hourly BTC (USD) from {start_dt.isoformat()} to {end_dt.isoformat()}")

    rows = []
    for s, e in chunks(start_dt, end_dt, days_chunk=MAX_DAYS_PER_REQUEST):
        from_unix = to_unix(s)
        to_unix_ts = to_unix(e)
        print(f"Requesting from {s.date()} to {e.date()} (unix {from_unix} - {to_unix_ts})")
        data = fetch_range(from_unix, to_unix_ts)
        # data contains 'prices' as [[ms, price], ...], and optionally 'market_caps'/'total_volumes'
        prices = data.get("prices", [])
        for p in prices:
            ts_ms = int(p[0])
            price = float(p[1])
            iso = datetime.fromtimestamp(ts_ms/1000.0, tz=timezone.utc).isoformat()
            rows.append((iso, ts_ms, price))
        # polite delay to avoid rate limits
        time.sleep(delay_between)

    # dedupe & sort by timestamp
    rows = sorted({r[1]: r for r in rows}.values(), key=lambda x: x[1])

    # write CSV
    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time_iso_utc", "time_unix_ms", "price_usd"])
        for iso, ms, price in rows:
            writer.writerow([iso, ms, f"{price:.8f}"])

    print(f"Wrote {len(rows)} rows -> {out}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=10)
    parser.add_argument("--out", type=str, default="btcusdt_1h_10months.csv")
    args = parser.parse_args()
    main(months=args.months, out=args.out)