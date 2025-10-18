#!/usr/bin/env python3
"""
get_btc_hourly_coingecko.py (with Binance fallback)

1) Try CoinGecko /market_chart/range to build hourly OHLCV.
2) If resulting OHLC bars are mostly flat (Open==High==Low==Close),
   automatically fallback to Binance public API and fetch real 1h klines.
3) Output CSV with columns:
    time_iso_utc,time_unix_ms,Open,High,Low,Close,Volume
"""
import requests
import time
from datetime import datetime, timezone, timedelta
import argparse
import sys
from typing import List, Tuple
import pandas as pd
import math

COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
MAX_DAYS_PER_REQUEST = 90
DEFAULT_DELAY = 0.25

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_LIMIT = 1000  # max bars per request

def to_unix(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def chunks(start_dt: datetime, end_dt: datetime, days_chunk: int = MAX_DAYS_PER_REQUEST):
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(days=days_chunk))
        yield cur, nxt
        cur = nxt

def fetch_coingecko_range(from_unix: int, to_unix: int, retries: int = 6):
    params = {"vs_currency": "usd", "from": str(from_unix), "to": str(to_unix)}
    headers = {"Accept": "application/json", "User-Agent": "cg-fetcher/1.0"}
    backoff = 2
    for attempt in range(retries):
        try:
            r = requests.get(COINGECKO_RANGE_URL, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503, 504):
                time.sleep(backoff ** attempt)
                continue
            r.raise_for_status()
        except Exception as e:
            time.sleep(backoff ** attempt)
    raise RuntimeError("CoinGecko fetch failed")

def build_ohlcv_from_samples(all_prices: List[Tuple[int,float]], all_vols: List[Tuple[int,float]]) -> pd.DataFrame:
    if not all_prices:
        raise RuntimeError("No price samples")
    dfp = pd.DataFrame(all_prices, columns=["time_ms","price"])
    dfp["time"] = pd.to_datetime(dfp["time_ms"] // 1000, unit="s", utc=True)
    dfp = dfp.set_index("time").sort_index()
    if all_vols:
        dfv = pd.DataFrame(all_vols, columns=["time_ms","vol"])
        dfv["time"] = pd.to_datetime(dfv["time_ms"] // 1000, unit="s", utc=True)
        dfv = dfv.set_index("time").sort_index()
        df = dfp.join(dfv["vol"], how="outer").sort_index()
        df["price"] = df["price"].ffill()
        df["vol"] = df["vol"].fillna(0)
    else:
        df = dfp.copy()
        df["vol"] = 0.0
    ohlc = df["price"].resample("h").ohlc()
    vol = df["vol"].resample("h").sum()
    df_ohlcv = ohlc.join(vol).dropna(subset=["close"])
    df_ohlcv.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","vol":"Volume"}, inplace=True)
    df_ohlcv = df_ohlcv.reset_index()
    df_ohlcv["time_iso_utc"] = df_ohlcv["time"].dt.tz_convert(timezone.utc).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df_ohlcv["time_unix_ms"] = (df_ohlcv["time"].astype("int64") // 10**6).astype(int)
    df_ohlcv = df_ohlcv[["time_iso_utc","time_unix_ms","Open","High","Low","Close","Volume"]]
    return df_ohlcv

def fetch_via_coingecko(start_dt: datetime, end_dt: datetime, delay_between=DEFAULT_DELAY) -> pd.DataFrame:
    all_prices = []
    all_vols = []
    for s,e in chunks(start_dt, end_dt, days_chunk=MAX_DAYS_PER_REQUEST):
        print(f"[CG] chunk {s.isoformat()} -> {e.isoformat()}")
        data = fetch_coingecko_range(to_unix(s), to_unix(e))
        prices = data.get("prices", [])
        vols = data.get("total_volumes", [])
        for p in prices:
            try:
                ts = int(p[0]); price = float(p[1]); all_prices.append((ts,price))
            except: pass
        for v in vols:
            try:
                ts = int(v[0]); vol = float(v[1]); all_vols.append((ts,vol))
            except: pass
        time.sleep(delay_between)
    # dedupe keep last
    price_map = {}
    for ts,price in all_prices: price_map[ts] = price
    vol_map = {}
    for ts,vol in all_vols: vol_map[ts] = vol
    prices_unique = sorted([(k, price_map[k]) for k in price_map.keys()])
    vols_unique = sorted([(k, vol_map[k]) for k in vol_map.keys()])
    df_ohlcv = build_ohlcv_from_samples(prices_unique, vols_unique)
    return df_ohlcv

# ---------------- Binance fetcher ----------------
# fetch klines from binance public REST (BTCUSDT, 1h). No API key needed for historical klines.
def fetch_binance_klines(start_dt: datetime, end_dt: datetime, symbol="BTCUSDT", interval="1h", delay_between=0.2) -> pd.DataFrame:
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rows = []
    url = BINANCE_KLINES_URL
    while start_ms < end_ms:
        params = {"symbol": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms, "limit": BINANCE_LIMIT}
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Binance error {r.status_code}: {r.text}")
        data = r.json()
        if not data:
            break
        for k in data:
            # k format: [openTime, open, high, low, close, volume, closeTime, ...]
            open_time = int(k[0])
            open_p = float(k[1]); high_p=float(k[2]); low_p=float(k[3]); close_p=float(k[4]); vol=float(k[5])
            rows.append((open_time, open_p, high_p, low_p, close_p, vol))
        # advance start_ms to last returned bar + 1 ms
        last_open = int(data[-1][0])
        start_ms = last_open + 1
        time.sleep(delay_between)
    if not rows:
        raise RuntimeError("No klines from Binance")
    df = pd.DataFrame(rows, columns=["time_ms","Open","High","Low","Close","Volume"])
    df["time"] = pd.to_datetime(df["time_ms"]//1000, unit="s", utc=True)
    df = df[["time","time_ms","Open","High","Low","Close","Volume"]]
    df["time_iso_utc"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df["time_unix_ms"] = df["time_ms"].astype(int)
    df = df[["time_iso_utc","time_unix_ms","Open","High","Low","Close","Volume"]]
    return df

# --------------- helper: detect flat OHLC ---------------
def flat_ohlc_ratio(df_ohlcv: pd.DataFrame) -> float:
    if df_ohlcv.empty: return 1.0
    flat = ((df_ohlcv["Open"] == df_ohlcv["High"]) & (df_ohlcv["Open"] == df_ohlcv["Low"]) & (df_ohlcv["Open"] == df_ohlcv["Close"]))
    return flat.sum() / len(df_ohlcv)

# ---------------- main ----------------
def main(months=10, out="btcusdt_1h_10months.csv", delay=DEFAULT_DELAY, chunk_days=MAX_DAYS_PER_REQUEST, cg_fallback_threshold=0.8):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # compute start as first of month months ago
    year = now.year; month = now.month - months
    while month <= 0:
        month += 12; year -= 1
    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = now
    print(f"[INFO] Range {start_dt} -> {end_dt}")

    # 1) try coingecko
    try:
        df_cg = fetch_via_coingecko(start_dt, end_dt, delay_between=delay)
        ratio = flat_ohlc_ratio(df_cg)
        print(f"[INFO] CoinGecko OHLC flat-ratio: {ratio:.3f} (threshold {cg_fallback_threshold})")
        if ratio < cg_fallback_threshold:
            print(f"[INFO] CoinGecko OK, writing {out}")
            df_cg.to_csv(out, index=False, float_format="%.8f")
            print(f"[INFO] Wrote {len(df_cg)} rows")
            return
        else:
            print("[WARN] CoinGecko produced mostly flat OHLC. Fallback to Binance klines.")
    except Exception as e:
        print(f"[WARN] CoinGecko fetch/build failed: {e}. Will fallback to Binance.", file=sys.stderr)

    # 2) fallback to Binance
    try:
        print("[INFO] Fetching Binance klines (1h)...")
        df_b = fetch_binance_klines(start_dt, end_dt, delay_between=delay)
        print(f"[INFO] Binance returned {len(df_b)} bars. Writing {out}")
        df_b.to_csv(out, index=False, float_format="%.8f")
        return
    except Exception as e:
        print(f"[ERROR] Binance fetch failed: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--months", type=int, default=10)
    p.add_argument("--out", type=str, default="btcusdt_1h_10months.csv")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    p.add_argument("--chunk-days", type=int, default=MAX_DAYS_PER_REQUEST)
    p.add_argument("--cg-fallback-threshold", type=float, default=0.8,
                   help="If fraction of flat OHLC from CoinGecko >= threshold, fallback to Binance")
    args = p.parse_args()
    main(months=args.months, out=args.out, delay=args.delay, chunk_days=args.chunk_days,
         cg_fallback_threshold=args.cg_fallback_threshold)