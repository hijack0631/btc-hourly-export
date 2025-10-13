#!/usr/bin/env python3
"""
get_btcusdt_hourly.py
Fetch hourly BTCUSDT klines from Binance for the past N months (default 10),
and write a CSV with ISO timestamps and OHLCV. Uses public REST endpoint:
GET /api/v3/klines
Docs: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
"""

import requests
import csv
import time
import math
from datetime import datetime, timezone, timedelta
import argparse
import sys
import pytz

BASE_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000

def ts_ms(dt: datetime):
    return int(dt.timestamp() * 1000)

def fetch_chunk(symbol, interval, start_ms, limit):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "limit": limit
    }
    for attempt in range(6):
        try:
            r = requests.get(BASE_URL, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            # 429 or 418 are rate-limit / banned responses -> backoff
            if r.status_code in (429, 418) or 500 <= r.status_code < 600:
                backoff = 2 ** attempt
                print(f"HTTP {r.status_code} - backing off {backoff}s (attempt {attempt+1})", file=sys.stderr)
                time.sleep(backoff)
                continue
            else:
                raise
        except requests.exceptions.RequestException as e:
            backoff = 2 ** attempt
            print(f"Network error: {e} - retrying in {backoff}s", file=sys.stderr)
            time.sleep(backoff)
    raise RuntimeError("Failed to fetch data after retries")

def main(
    symbol="BTCUSDT",
    months=10,
    interval="1h",
    end_utc=None,
    tz_out="UTC",
    out_csv="btcusdt_1h_10months.csv",
    sleep_between=0.35
):
    # Determine end / start datetimes (use UTC internal)
    if end_utc is None:
        end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    else:
        end_dt = end_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # Subtract calendar months roughly: we'll subtract months by adjusting year/month
    # Simple approach: go back `months` by decrementing month with carry
    year = end_dt.year
    month = end_dt.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(end_dt.day, 28)  # safe anchor (we align at midnight)
    start_dt = datetime(year, month, day, tzinfo=timezone.utc)
    # For safety, set start to midnight of that day
    start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"Fetching {symbol} {interval} from {start_dt.isoformat()} to {end_dt.isoformat()} (UTC) ...")

    start_ms = ts_ms(start_dt)
    end_ms = ts_ms(end_dt)

    all_rows = []
    next_start_ms = start_ms
    total_expected_hours = int((end_dt - start_dt).total_seconds() // 3600)
    print(f"Expected data points (hours): ~{total_expected_hours}")

    while next_start_ms < end_ms:
        # we request up to MAX_LIMIT candles starting at next_start_ms
        chunk = fetch_chunk(symbol, interval, next_start_ms, MAX_LIMIT)
        if not chunk:
            print("Empty chunk returned — stopping", file=sys.stderr)
            break
        # chunk is an array of arrays: [ openTime, open, high, low, close, volume, closeTime, ... ]
        for k in chunk:
            open_time_ms = int(k[0])
            open_iso = datetime.fromtimestamp(open_time_ms/1000.0, tz=timezone.utc).isoformat()
            close_price = k[4]
            open_price = k[1]
            high = k[2]
            low = k[3]
            volume = k[5]
            close_time_ms = int(k[6])
            all_rows.append({
                "open_time_utc": open_iso,
                "open_ms": open_time_ms,
                "close_ms": close_time_ms,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close_price,
                "volume": volume
            })
        # Advance next_start_ms: the last candle's openTime + interval
        last_open_ms = int(chunk[-1][0])
        # interval in ms (1h)
        interval_ms = 3600 * 1000
        next_start_ms = last_open_ms + interval_ms

        # small polite sleep to avoid hitting rate-limits; Binance allows reasonable public usage,
        # but don't hammer (adjust sleep_between if you like).
        time.sleep(sleep_between)

        # safety: break if no progress
        if next_start_ms <= start_ms:
            print("No progress in pagination; aborting.", file=sys.stderr)
            break

        # stop if we've reached or passed end_ms
        if next_start_ms >= end_ms:
            break

    # Convert times to desired timezone for output if requested
    out_tz = pytz.timezone(tz_out) if tz_out and tz_out.upper() != "UTC" else timezone.utc

    # Write CSV
    fieldnames = ["open_time_iso", "open_time_ms", "open","high","low","close","volume","close_time_ms"]
    with open(out_csv, "w", newline='') as f:
        w = csv.writer(f)
        w.writerow(fieldnames)
        for r in all_rows:
            dt_utc = datetime.fromtimestamp(r["open_ms"]/1000.0, tz=timezone.utc)
            if out_tz == timezone.utc:
                iso = dt_utc.isoformat()
            else:
                iso = dt_utc.astimezone(out_tz).isoformat()
            w.writerow([iso, r["open_ms"], r["open"], r["high"], r["low"], r["close"], r["volume"], r["close_ms"]])

    print(f"Wrote {len(all_rows)} rows to {out_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download hourly BTCUSDT klines for last N months from Binance.")
    parser.add_argument("--months", type=int, default=10, help="How many months back to fetch (default 10)")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol (default BTCUSDT)")
    parser.add_argument("--interval", type=str, default="1h", help="Kline interval (default 1h)")
    parser.add_argument("--out", type=str, default="btcusdt_1h_10months.csv", help="Output CSV filename")
    parser.add_argument("--tz", type=str, default="UTC", help="Output timezone (e.g. Europe/Zurich)")
    args = parser.parse_args()
    # compute end_utc as now
    tz = pytz.timezone(args.tz) if args.tz else None
    main(symbol=args.symbol, months=args.months, interval=args.interval, tz_out=args.tz, out_csv=args.out)