#!/usr/bin/env python3
"""
get_btc_hourly_for_grid_research.py

1) Try CoinGecko /market_chart/range to build hourly OHLCV.
2) If CoinGecko bars are mostly flat OR CoinGecko fails -> fallback to Bitfinex public candles API.
3) Output CSV: time_iso_utc,time_unix_ms,Open,High,Low,Close,Volume

Bitfinex candles docs: public candles endpoint supports timeframe=1h and start/end/limit parameters.
"""
import requests, time, csv, sys
from datetime import datetime, timezone, timedelta
import argparse
from typing import List, Tuple
import pandas as pd

# --- Config
COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
MAX_DAYS_PER_CG = 90
CG_DELAY = 0.25

BITFINEX_CANDLES_URL = "https://api-pub.bitfinex.com/v2/candles/trade:1h:tBTCUSD/hist"
BF_LIMIT = 1000
BF_DELAY = 0.25  # seconds between Bitfinex requests (rate-limit safety)

FLAT_THRESHOLD_DEFAULT = 0.8

# --- Helpers
def to_unix(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def chunks(start_dt: datetime, end_dt: datetime, days_chunk: int = MAX_DAYS_PER_CG):
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(days=days_chunk))
        yield cur, nxt
        cur = nxt

# --- CoinGecko fetch + OHLC build (same approach)
def fetch_coingecko_range(from_unix: int, to_unix: int, retries: int = 6):
    params = {"vs_currency":"usd","from":str(from_unix),"to":str(to_unix)}
    headers = {"Accept":"application/json","User-Agent":"cg-fetcher/1.0"}
    backoff = 2
    for attempt in range(retries):
        try:
            r = requests.get(COINGECKO_RANGE_URL, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            # 429, 5xx -> backoff
            if r.status_code in (429,502,503,504):
                time.sleep(backoff ** attempt)
                continue
            r.raise_for_status()
        except requests.RequestException:
            time.sleep(backoff ** attempt)
    raise RuntimeError("CoinGecko fetch failed")

def build_ohlcv_from_samples(all_prices: List[Tuple[int,float]], all_vols: List[Tuple[int,float]]) -> pd.DataFrame:
    if not all_prices:
        raise RuntimeError("No price samples")
    dfp = pd.DataFrame(all_prices, columns=["time_ms","price"])
    dfp["time"] = pd.to_datetime(dfp["time_ms"]//1000, unit="s", utc=True)
    dfp = dfp.set_index("time").sort_index()
    if all_vols:
        dfv = pd.DataFrame(all_vols, columns=["time_ms","vol"])
        dfv["time"] = pd.to_datetime(dfv["time_ms"]//1000, unit="s", utc=True)
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

def fetch_via_coingecko(start_dt: datetime, end_dt: datetime, delay_between=CG_DELAY) -> pd.DataFrame:
    all_prices=[]; all_vols=[]
    for s,e in chunks(start_dt, end_dt, days_chunk=MAX_DAYS_PER_CG):
        print(f"[CG] chunk {s.isoformat()} -> {e.isoformat()}")
        data = fetch_coingecko_range(to_unix(s), to_unix(e))
        prices = data.get("prices", [])
        vols = data.get("total_volumes", [])
        for p in prices:
            try: all_prices.append((int(p[0]), float(p[1])))
            except: pass
        for v in vols:
            try: all_vols.append((int(v[0]), float(v[1])))
            except: pass
        time.sleep(delay_between)
    # dedupe (keep last)
    price_map={}; vol_map={}
    for ts,pr in all_prices: price_map[ts]=pr
    for ts,vo in all_vols: vol_map[ts]=vo
    prices_unique = sorted([(k, price_map[k]) for k in price_map.keys()])
    vols_unique = sorted([(k, vol_map[k]) for k in vol_map.keys()])
    df_ohlcv = build_ohlcv_from_samples(prices_unique, vols_unique)
    return df_ohlcv

# --- Bitfinex fetcher (public candles)
def fetch_bitfinex_candles(start_dt: datetime, end_dt: datetime, delay_between=BF_DELAY):
    """
    Fetch 1h candles from Bitfinex public endpoint using paging.
    Endpoint: /v2/candles/trade:1h:tBTCUSD/hist
    Params: start (ms), end (ms), limit (<=1000), sort (1 for ascending)
    """
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    rows=[]
    current_start = start_ms
    headers = {"User-Agent":"bf-fetcher/1.0"}
    while current_start < end_ms:
        params = {"start": current_start, "end": end_ms, "limit": BF_LIMIT, "sort": 1}
        r = requests.get(BITFINEX_CANDLES_URL, params=params, headers=headers, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Bitfinex error {r.status_code}: {r.text}")
        data = r.json()
        if not data:
            break
        # data items: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME] per docs
        for item in data:
            mts = int(item[0])
            open_p = float(item[1]); close_p = float(item[2])
            high_p = float(item[3]); low_p = float(item[4]); vol = float(item[5])
            rows.append((mts, open_p, high_p, low_p, close_p, vol))
        last_mts = int(data[-1][0])
        # advance: next start after last_mts
        current_start = last_mts + 1
        time.sleep(delay_between)
    if not rows:
        raise RuntimeError("Bitfinex returned no candles")
    df = pd.DataFrame(rows, columns=["time_unix_ms","Open","High","Low","Close","Volume"])
    df["time"] = pd.to_datetime(df["time_unix_ms"]//1000, unit="s", utc=True)
    df["time_iso_utc"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df = df[["time_iso_utc","time_unix_ms","Open","High","Low","Close","Volume"]]
    # remove duplicates & keep sorted
    df = df.drop_duplicates(subset=["time_unix_ms"]).sort_values("time_unix_ms").reset_index(drop=True)
    return df

def flat_ohlc_ratio(df_ohlcv):
    if df_ohlcv.empty: return 1.0
    flat = ((df_ohlcv["Open"]==df_ohlcv["High"]) & (df_ohlcv["Open"]==df_ohlcv["Low"]) & (df_ohlcv["Open"]==df_ohlcv["Close"]))
    return flat.sum() / len(df_ohlcv)

# --- Main workflow
def main(months=10, out="btcusdt_1h_10months.csv", cg_fallback_threshold=FLAT_THRESHOLD_DEFAULT):
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    year = now.year; month = now.month - months
    while month <= 0:
        month += 12; year -= 1
    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = now
    print(f"[RUN] Range {start_dt.isoformat()} -> {end_dt.isoformat()}")

    # 1) try CoinGecko
    try:
        df_cg = fetch_via_coingecko(start_dt, end_dt) if False else fetch_via_coingecko(start_dt, end_dt)  # keep same function name
        ratio = flat_ohlc_ratio(df_cg)
        print(f"[INFO] CoinGecko flat OHLC ratio: {ratio:.3f}")
        if ratio < cg_fallback_threshold:
            print(f"[INFO] Using CoinGecko OHLC (flat ratio {ratio:.3f} < {cg_fallback_threshold}) -> write {out}")
            df_cg.to_csv(out, index=False, float_format="%.8f")
            return
        else:
            print(f"[WARN] CoinGecko produced mostly flat OHLC (ratio {ratio:.3f}) -> fallback to Bitfinex")
    except Exception as e:
        print(f"[WARN] CoinGecko failed: {e}. Falling back to Bitfinex.", file=sys.stderr)

    # 2) Bitfinex fallback
    try:
        print("[INFO] Fetching Bitfinex 1h candles...")
        df_bf = fetch_bitfinex_candles(start_dt, end_dt)
        print(f"[INFO] Bitfinex returned {len(df_bf)} bars -> write {out}")
        df_bf.to_csv(out, index=False, float_format="%.8f")
        return
    except Exception as e:
        print(f"[ERROR] Bitfinex fetch failed: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--months", type=int, default=10)
    p.add_argument("--out", type=str, default="btcusdt_1h_10months.csv")
    p.add_argument("--cg-fallback-threshold", type=float, default=FLAT_THRESHOLD_DEFAULT)
    args = p.parse_args()
    main(months=args.months, out=args.out, cg_fallback_threshold=args.cg_fallback_threshold)