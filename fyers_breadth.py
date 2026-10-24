"""
NSE market breadth built on the Fyers API.

Same idea as the NSE-bhavcopy script, but prices/volumes come from Fyers
daily candles. Fyers has NO delivery % data, so the DELIV breadth is replaced
with a VOLUME breadth (stock volume vs its own 3M/6M/1Y volume averages).
If you need real DELIV_PER, keep pulling that from NSE bhavcopy.

Setup:
    pip install fyers-apiv3 pandas requests
    export FYERS_APP_ID="XXXXXXXXXX-100"        # your API app id
    export FYERS_ACCESS_TOKEN="eyJ..."          # today's access token (expires daily)
    (or just paste them into the script near the top)

Rate limits (Data APIs, per Fyers Plans & Limits page):
    Standard: 5/sec, 50/min, 5,000/day     Prime: 10/sec, 500/min, 5,00,000/day
Exceeding the per-minute limit more than 3 times in a day blocks you for the day,
so this script paces itself and keeps a persistent per-day request counter.

On Standard a cold first run needs ~6,000 requests -> it stops at the daily budget,
saves everything to cache, and you simply re-run it the next day to finish.
Later runs are incremental (about 1 request per symbol, only when new days exist).

Test first:   python fyers_breadth.py --limit 20
Full run:     python fyers_breadth.py
"""
import os
import re
import sys
import json
import time
import argparse
from datetime import datetime, timedelta

import pandas as pd
import requests
from fyers_apiv3 import fyersModel

# ==========================================
# CONFIGURATION - EDIT THESE
# ==========================================
MA_3M_DAYS = 63
MA_6M_DAYS = 126
MA_1Y_DAYS = 252
MIN_PERIODS = {"3m": 30, "6m": 60, "1y": 120}

REPORT_START = datetime.now() - timedelta(days=365)
REPORT_END = datetime.now()
WARMUP_DAYS = 450
DATA_START = REPORT_START - timedelta(days=WARMUP_DAYS)

CACHE_DIR = "fyers_cache"
OUT_DIR = "fyers_breadth_outputs"
MONTHLY_REPORT_CSV = os.path.join(OUT_DIR, "monthly_breadth_report.csv")
DAILY_REPORT_CSV = os.path.join(OUT_DIR, "daily_breadth_report.csv")
USAGE_FILE = os.path.join(CACHE_DIR, "api_usage.json")

SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
CHUNK_DAYS = 360            # daily resolution allows up to ~366 days per request
DROP_INCOMPLETE_TODAY = True  # drop today's candle if run before 4 PM

# ------------------------------------------------------------------
# >>> PASTE YOUR FYERS CREDENTIALS HERE (the access token expires daily) <<<
# If these are left empty, the script falls back to environment variables.
# ------------------------------------------------------------------
FYERS_APP_ID = "J0ENYFX9ME-200"            # e.g. "XXXXXXXXXX-200"
FYERS_ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCcTNGblVOQjhlR1d4c1RRLXFlaEdGTVR1NkdmSzJfYl85cWlFVm8yLVEyU1RGajNEVnlQR0ZYc0RiMzgtMkxpVWk2ZFdFYjdnVnNaT3Q1T0FxT3A0RlItc0t4V3JOd3lWblpUWEI2UmFvSi1XM1lHbz0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI0NDIxYTAyNDU1ZWQyOTg3NzZhYzdiMzRmYzAzODJmYjk2MTE4Y2M1NzE3Y2NlMWE0ZWJkYmU0YyIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiRkFBMzE0OTUiLCJhcHBUeXBlIjoyMDAsImV4cCI6MTc5Mjg4ODIwMCwiaWF0IjoxNzkyODI1ODEyLCJpc3MiOiJhcGkuZnllcnMuaW4iLCJuYmYiOjE3OTI4MjU4MTIsInN1YiI6ImFjY2Vzc190b2tlbiJ9.bqZX1INNQ5I0GI7V5K9AcpVFsK9RqbFFGUFC6pIWZjk"
FYERS_PLAN = "prime"                          # "prime" or "standard"


def _cfg(value, env_name, default=""):
    if value and not value.startswith("PASTE_"):
        return value
    return os.getenv(env_name, default)


PLAN = _cfg(FYERS_PLAN, "FYERS_PLAN", "prime").strip().lower()

# Plan-based pacing, kept safely under the real limits
if PLAN == "prime":
    MIN_CALL_INTERVAL = 0.2     # ~300/min (limit 500/min, 10/sec)
    DAILY_BUDGET = 450_000      # limit 5,00,000
else:
    MIN_CALL_INTERVAL = 1.4     # ~43/min (limit 50/min, 5/sec)
    DAILY_BUDGET = 4_800        # limit 5,000

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)


class BudgetExhausted(Exception):
    pass


# ==========================================
# THROTTLE + PERSISTENT DAILY COUNTER
# ==========================================
_last_call = [0.0]


def _load_usage():
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        with open(USAGE_FILE) as f:
            u = json.load(f)
        if u.get("date") == today:
            return u
    except Exception:
        pass
    return {"date": today, "count": 0}


def _save_usage(u):
    with open(USAGE_FILE, "w") as f:
        json.dump(u, f)


def requests_left():
    return DAILY_BUDGET - _load_usage()["count"]


def throttle():
    """Wait for the pacing interval and count the request. Raises when the daily budget is gone."""
    u = _load_usage()
    if u["count"] >= DAILY_BUDGET:
        raise BudgetExhausted()
    u["count"] += 1
    _save_usage(u)
    wait = MIN_CALL_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait)
    _last_call[0] = time.time()


# ==========================================
# SYMBOL UNIVERSE (all NSE -EQ stocks from Fyers symbol master)
# ==========================================
def load_symbols():
    path = os.path.join(CACHE_DIR, "NSE_CM.csv")
    stale = (not os.path.exists(path)) or (time.time() - os.path.getmtime(path) > 86400)
    if stale:
        r = requests.get(SYMBOL_MASTER_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(path, header=None, dtype=str)
    pattern = r"^NSE:\S+-EQ$"
    # The file has no header row, so find the column holding tickers like "NSE:SBIN-EQ"
    for col in df.columns:
        s = df[col].dropna()
        hits = s[s.str.match(pattern)]
        if len(hits) > 500:
            return sorted(hits.unique())
    raise RuntimeError("Could not find ticker column in the Fyers symbol master file.")


# ==========================================
# FYERS HISTORY FETCH (incremental, cached per symbol)
# ==========================================
def safe_name(symbol):
    return re.sub(r"[^A-Za-z0-9_\-]", "_", symbol.replace("&", "_AND_"))


def fetch_chunk(fyers, symbol, start, end):
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",  # range_from/range_to as YYYY-MM-DD
        "range_from": start.strftime("%Y-%m-%d"),
        "range_to": end.strftime("%Y-%m-%d"),
        "cont_flag": "1",
    }
    for attempt in range(5):
        throttle()  # may raise BudgetExhausted
        try:
            resp = fyers.history(data=data)
        except Exception as e:
            print(f"  ! {symbol}: exception {e}; retrying")
            time.sleep(2 * (attempt + 1))
            continue
        status = resp.get("s")
        if status == "ok":
            return resp.get("candles", [])
        if status == "no_data":
            return []
        msg = str(resp.get("message", "")).lower()
        code = resp.get("code")
        if code in (429, -429) or "limit" in msg or "many" in msg:
            print("  ! rate limited, sleeping 70s (repeated minute-limit hits get you blocked for the day)")
            time.sleep(70)
            continue
        if "token" in msg or "authenticate" in msg or "expired" in msg:
            sys.exit(f"Auth problem: {resp}. Generate a fresh access token and re-run.")
        print(f"  ! {symbol}: {resp}")
        time.sleep(2 * (attempt + 1))
    return []


def candles_to_df(candles):
    # candle = [epoch_seconds, open, high, low, close, volume]
    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    # Daily candles come stamped at 00:00 UTC of the trading date
    df["date"] = pd.to_datetime(df["ts"], unit="s").dt.normalize()
    return df[["date", "open", "high", "low", "close", "volume"]]


def last_expected_trading_day(now):
    """Most recent weekday whose daily candle should be final (ignores exchange holidays)."""
    d = now.date()
    if now.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return pd.Timestamp(d)


def update_symbol(fyers, symbol, end):
    path = os.path.join(CACHE_DIR, safe_name(symbol) + ".csv")
    cached = None
    start = DATA_START
    if os.path.exists(path):
        try:
            cached = pd.read_csv(path, parse_dates=["date"])
            if not cached.empty:
                if cached["date"].max() >= last_expected_trading_day(end):
                    return cached  # already up to date -> 0 requests
                start = cached["date"].max().to_pydatetime() + timedelta(days=1)
        except Exception:
            cached = None
    if start.date() > end.date():
        return cached

    # Plan the chunks first so a symbol is never left half-downloaded when the budget runs out
    chunks = []
    cur = start
    while cur.date() <= end.date():
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    if requests_left() < len(chunks):
        raise BudgetExhausted()

    frames = []
    for a, b in chunks:
        candles = fetch_chunk(fyers, symbol, a, b)
        if candles:
            frames.append(candles_to_df(candles))

    if cached is not None:
        frames.insert(0, cached)
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset="date", keep="last").sort_values("date")
    df.to_csv(path, index=False)
    return df


def download_all(fyers, symbols):
    end = datetime.now()
    t0 = time.time()
    data = {}
    done = 0
    for i, sym in enumerate(symbols, 1):
        try:
            df = update_symbol(fyers, sym, end)
        except BudgetExhausted:
            print(f"\nDaily request budget ({DAILY_BUDGET}) reached after {done}/{len(symbols)} symbols.")
            print("Everything downloaded so far is cached. Re-run tomorrow to continue where this stopped.")
            return data, False
        done = i
        if df is not None and not df.empty:
            data[sym] = df
        if i % 25 == 0 or i == len(symbols):
            elapsed = time.time() - t0
            eta = elapsed / i * (len(symbols) - i)
            print(f"[{i}/{len(symbols)}] {sym}  requests left today: {requests_left()}  "
                  f"elapsed {elapsed/60:.1f}m  eta {eta/60:.1f}m")
    return data, True


def build_matrices(data):
    close = pd.DataFrame({s: d.set_index("date")["close"] for s, d in data.items()}).sort_index()
    volume = pd.DataFrame({s: d.set_index("date")["volume"] for s, d in data.items()}).sort_index()
    if DROP_INCOMPLETE_TODAY:
        now = datetime.now()
        if now.hour < 16:
            today = pd.Timestamp(now.date())
            close = close[close.index < today]
            volume = volume[volume.index < today]
    close.to_csv(os.path.join(OUT_DIR, "price_matrix.csv"))
    volume.to_csv(os.path.join(OUT_DIR, "volume_matrix.csv"))
    return close, volume


# ==========================================
# BREADTH (works the same for price and volume)
# ==========================================
def breadth(matrix, suffix):
    ma3 = matrix.rolling(MA_3M_DAYS, min_periods=MIN_PERIODS["3m"]).mean()
    ma6 = matrix.rolling(MA_6M_DAYS, min_periods=MIN_PERIODS["6m"]).mean()
    ma1 = matrix.rolling(MA_1Y_DAYS, min_periods=MIN_PERIODS["1y"]).mean()

    valid = matrix.notna() & ma3.notna() & ma6.notna() & ma1.notna()
    above = (matrix > ma3) & (matrix > ma6) & (matrix > ma1) & valid
    below = (matrix < ma3) & (matrix < ma6) & (matrix < ma1) & valid

    out = pd.DataFrame(index=matrix.index)
    out[f"bullish_{suffix}"] = above.sum(axis=1)
    out[f"bearish_{suffix}"] = below.sum(axis=1)
    out[f"total_{suffix}"] = valid.sum(axis=1)
    out[f"sideways_{suffix}"] = out[f"total_{suffix}"] - out[f"bullish_{suffix}"] - out[f"bearish_{suffix}"]
    tot = out[f"total_{suffix}"].replace(0, float("nan"))
    out[f"bull_pct_{suffix}"] = 100 * out[f"bullish_{suffix}"] / tot
    out[f"bear_pct_{suffix}"] = 100 * out[f"bearish_{suffix}"] / tot
    out[f"side_pct_{suffix}"] = 100 * out[f"sideways_{suffix}"] / tot
    return out.fillna(0)


def compute_daily_breadth(close, volume):
    p = breadth(close, "p")
    v = breadth(volume, "v")
    df = p.join(v)
    return df[df["total_p"] > 0]


# ==========================================
# REPORTS
# ==========================================
def generate_reports(df, start, end):
    df = df[(df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))]
    monthly = df.resample("ME").mean().round(2)
    monthly.index = monthly.index.strftime("%b-%y")
    monthly.to_csv(MONTHLY_REPORT_CSV)
    daily = df.round(2)
    daily.to_csv(DAILY_REPORT_CSV)

    names = ["Avg Bull", "Avg Bear", "Avg Side", "Avg Total", "Bull %", "Bear %", "Side %"]
    for title, s in [("Price Breadth", "p"), ("Volume Breadth", "v")]:
        cols = [f"bullish_{s}", f"bearish_{s}", f"sideways_{s}", f"total_{s}",
                f"bull_pct_{s}", f"bear_pct_{s}", f"side_pct_{s}"]
        t = monthly[cols].copy()
        t.columns = names
        print(f"\n--- {title} ---")
        print(t.to_string())
    print(f"\nSaved: {MONTHLY_REPORT_CSV}\n       {DAILY_REPORT_CSV}")
    return monthly, daily


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="only process the first N symbols (for testing)")
    ap.add_argument("--partial", action="store_true",
                    help="build reports from the symbols downloaded so far even if the run is incomplete")
    args = ap.parse_args()

    app_id = _cfg(FYERS_APP_ID, "FYERS_APP_ID")
    token = _cfg(FYERS_ACCESS_TOKEN, "FYERS_ACCESS_TOKEN")
    if not app_id or not token:
        sys.exit("Paste your FYERS_APP_ID and FYERS_ACCESS_TOKEN near the top of the script.")

    fyers = fyersModel.FyersModel(client_id=app_id, token=token, is_async=False, log_path="")

    symbols = load_symbols()
    if args.limit:
        symbols = symbols[: args.limit]
    est_calls = len(symbols) * (int((datetime.now() - DATA_START).days / CHUNK_DAYS) + 1)
    print(f"Plan: {PLAN} | pacing {60 / MIN_CALL_INTERVAL:.0f} req/min | daily budget {DAILY_BUDGET} "
          f"(left today: {requests_left()})")
    print(f"{len(symbols)} symbols, up to ~{est_calls} requests on a cold cache "
          f"(~{est_calls * MIN_CALL_INTERVAL / 60:.0f} min, "
          f"{max(1, -(-est_calls // DAILY_BUDGET))} day(s) at this plan's daily cap).")

    data, complete = download_all(fyers, symbols)
    if not complete and not args.partial:
        sys.exit("Download incomplete - re-run tomorrow (or use --partial to report on what's cached).")
    if not data:
        sys.exit("No data downloaded.")
    close, volume = build_matrices(data)
    print(f"Price matrix: {close.shape}, range {close.index.min().date()} -> {close.index.max().date()}")

    breadth_df = compute_daily_breadth(close, volume)
    generate_reports(breadth_df, REPORT_START, REPORT_END)