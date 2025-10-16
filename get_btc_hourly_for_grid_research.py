#!/usr/bin/env python3
"""
get_btc_hourly_coingecko.py

Fetch hourly BTC OHLCV (USD) for the last N months from CoinGecko using /coins/bitcoin/market_chart/range.
- Uses <= chunk-days windows (default 90 days) to get hourly granularity.
- Builds OHLCV by aggregating returned price & volume samples into hourly bars.
- Outputs CSV with columns:
    time_iso_utc, time_unix_ms, Open, High, Low, Close, Volume

Usage:
    python get_btc_hourly_coingecko.py --months 10 --out btcusdt_1h_10months.csv
"""
import requests
import csv
import time
from datetime import datetime, timezone, timedelta
import argparse
import sys
from typing import List, Tuple
import pandas as pd

COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
MAX_DAYS_PER_REQUEST = 90  # CoinGecko provides hourly granularity for <=90 days windows
DEFAULT_DELAY = 0.25  # seconds between requests to be polite

def to_unix(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def chunks(start_dt: datetime, end_dt: datetime, days_chunk: int = MAX_DAYS_PER_REQUEST):
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(days=days_chunk))
        yield cur, nxt
        cur = nxt

def fetch_range(from_unix: int, to_unix: int, retries: int = 6):
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
            print(f"[WARN] HTTP {r.status_code} from CoinGecko: {r.text[:800]}", file=sys.stderr)
            if r.status_code in (429, 502, 503, 504):
                sleep = backoff_base ** attempt
                print(f"[INFO] Backoff {sleep}s (attempt {attempt+1})", file=sys.stderr)
                time.sleep(sleep)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            sleep = backoff_base ** attempt
            print(f"[WARN] Request error: {e} - sleeping {sleep}s (attempt {attempt+1})", file=sys.stderr)
            time.sleep(sleep)
    raise RuntimeError("Failed to fetch data from CoinGecko after retries")

def build_ohlcv_from_chunks(all_prices: List[Tuple[int, float]], all_vols: List[Tuple[int, float]]) -> pd.DataFrame:
    """
    Build an hourly OHLCV DataFrame from lists of (timestamp_ms, price) and (timestamp_ms, volume).
    Returns DataFrame indexed by UTC datetime with columns: Open, High, Low, Close, Volume
    """
    # Create DataFrame for prices
    if not all_prices:
        raise RuntimeError("No price samples to build OHLCV from")
    df_price = pd.DataFrame(all_prices, columns=["time_ms", "price"])
    df_price["time"] = pd.to_datetime(df_price["time_ms"] // 1000, unit="s", utc=True)
    df_price = df_price.set_index("time").sort_index()

    # Create DataFrame for volumes (if provided)
    if all_vols:
        df_vol = pd.DataFrame(all_vols, columns=["time_ms", "volume"])
        df_vol["time"] = pd.to_datetime(df_vol["time_ms"] // 1000, unit="s", utc=True)
        df_vol = df_vol.set_index("time").sort_index()
        # join price points with volume points (outer join), fill missing volume=0 at price timestamps
        df = df_price.join(df_vol["volume"], how="outer").sort_index()
        df["price"] = df["price"].ffill()  # forward-fill price if volume-only timestamps existed
        df["volume"] = df["volume"].fillna(0)
    else:
        df = df_price.copy()
        df["volume"] = 0.0

    # Resample to hourly OHLC + sum(Volume)
    # If the incoming samples are already hourly, this results in exact bars.
    ohlc = df["price"].resample("h").ohlc()
    vol = df["volume"].resample("h").sum()
    df_ohlcv = ohlc.join(vol).dropna(subset=["close"])  # drop hours with no price
    df_ohlcv.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
    # create time columns for CSV
    df_ohlcv = df_ohlcv.reset_index()
    df_ohlcv["time_iso_utc"] = df_ohlcv["time"].dt.tz_convert(timezone.utc).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df_ohlcv["time_unix_ms"] = (df_ohlcv["time"].astype("int64") // 10**6).astype(int)
    # reorder columns
    df_ohlcv = df_ohlcv[["time_iso_utc", "time_unix_ms", "Open", "High", "Low", "Close", "Volume"]]
    return df_ohlcv

def main(months=10, out="btcusdt_1h_10months.csv", delay_between=DEFAULT_DELAY, chunk_days=MAX_DAYS_PER_REQUEST):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # compute start by subtracting months (approx safe: set to first of month)
    year = now.year
    month = now.month - months
    while month <= 0:
        month += 12
        year -= 1
    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = now

    print(f"[INFO] Fetching BTC price samples from {start_dt.isoformat()} to {end_dt.isoformat()} (UTC)")

    all_prices = []  # list of tuples (ms, price)
    all_vols = []    # list of tuples (ms, volume)
    for s, e in chunks(start_dt, end_dt, days_chunk=chunk_days):
        from_unix = to_unix(s)
        to_unix_ts = to_unix(e)
        print(f"[INFO] Request chunk: {s.isoformat()} -> {e.isoformat()}  (from={from_unix} to={to_unix_ts})")
        data = fetch_range(from_unix, to_unix_ts)
        if not data:
            print("[ERROR] Empty response for chunk — aborting.", file=sys.stderr)
            raise RuntimeError("Empty response from CoinGecko")
        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])  # pair list of [ts_ms, vol]
        # collect
        for p in prices:
            try:
                ts_ms = int(p[0])
                price = float(p[1])
                all_prices.append((ts_ms, price))
            except Exception:
                continue
        for v in volumes:
            try:
                ts_ms = int(v[0])
                vol = float(v[1])
                all_vols.append((ts_ms, vol))
            except Exception:
                continue
        time.sleep(delay_between)

    # Deduplicate by timestamp (keep last)
    if not all_prices:
        raise RuntimeError("No prices fetched.")
    price_map = {}
    for ts_ms, price in all_prices:
        price_map[ts_ms] = price
    prices_unique = sorted([(k, price_map[k]) for k in price_map.keys()])

    vol_map = {}
    for ts_ms, vol in all_vols:
        vol_map[ts_ms] = vol
    vols_unique = sorted([(k, vol_map[k]) for k in vol_map.keys()])

    # Build OHLCV DataFrame
    df_ohlcv = build_ohlcv_from_chunks(prices_unique, vols_unique)

    # final write CSV
    df_ohlcv.to_csv(out, index=False, float_format="%.8f")
    print(f"[INFO] Wrote {len(df_ohlcv)} hourly rows -> {out}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=10, help="How many months back to fetch")
    parser.add_argument("--out", type=str, default="btcusdt_1h_10months.csv", help="Output CSV filename")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between chunk requests (seconds)")
    parser.add_argument("--chunk-days", type=int, default=MAX_DAYS_PER_REQUEST, help="Days per chunk (<=90 recommended)")
    args = parser.parse_args()
    main(months=args.months, out=args.out, delay_between=args.delay, chunk_days=args.chunk_days)