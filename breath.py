import os
import io
import time
import pandas as pd
import requests
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION – EDIT THESE
# ==========================================
MA_3M_DAYS = 63
MA_6M_DAYS = 126
MA_1Y_DAYS = 252

MA_DELIV_3M_DAYS = 63
MA_DELIV_6M_DAYS = 126
MA_DELIV_1Y_DAYS = 252

OUT_DIR = "nse_breadth_outputs"
PRICE_MATRIX_CSV = f"{OUT_DIR}/nse_full_price_matrix.csv"
DELIV_MATRIX_CSV = f"{OUT_DIR}/nse_full_deliv_matrix.csv"
PREV_MATRIX_CSV = f"{OUT_DIR}/nse_full_prev_matrix.csv"   # NEW: PREV_CLOSE (split/bonus adjusted by NSE)
CACHE_DIR = "nse_bhavcopy_cache"

MONTHLY_REPORT_CSV = f"{OUT_DIR}/monthly_breadth_report.csv"
DAILY_REPORT_CSV = f"{OUT_DIR}/daily_breadth_report.csv"

# CHANGED: 5 years of reporting
REPORT_START = datetime.now() - timedelta(days=365 * 5)
REPORT_END = datetime.now()

WARMUP_DAYS = 380
DATA_START = REPORT_START - timedelta(days=WARMUP_DAYS)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/all-reports",
}

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

# ==========================================
# NSE BHAVCOPY FETCHING
# ==========================================
def get_nse_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=10)
    except Exception as e:
        print(f"Warning: Failed to initialize NSE session cookies: {e}")
    return session

def fetch_bhavcopy(session, date_obj):
    date_str_api = date_obj.strftime("%d-%b-%Y")
    date_str_archive = date_obj.strftime("%d%m%Y")
    file_path = os.path.join(CACHE_DIR, f"sec_bhavdata_full_{date_str_archive}.csv")
    if os.path.exists(file_path):
        try:
            cached = pd.read_csv(file_path)
            cached.columns = cached.columns.str.strip()
            return cached
        except Exception:
            pass

    api_url = (
        f"https://www.nseindia.com/api/reports?archives="
        f"%5B%7B%22name%22%3A%22Full%20Bhavcopy%20and%20Security%20Deliverable%20data%22%2C"
        f"%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C"
        f"%22section%22%3A%22equities%22%7D%5D&date={date_str_api}&type=equities&mode=single"
    )
    df = None
    try:
        resp = session.get(api_url, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0 and 'fileUrl' in data[0]:
                file_url = data[0]['fileUrl']
                file_resp = session.get(file_url, timeout=8)
                if file_resp.status_code == 200:
                    df = pd.read_csv(io.StringIO(file_resp.text))
    except Exception:
        df = None

    if df is None:
        archive_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str_archive}.csv"
        try:
            resp = session.get(archive_url, timeout=8)
            if resp.status_code == 200 and "SYMBOL" in resp.text:
                df = pd.read_csv(io.StringIO(resp.text))
        except Exception:
            df = None

    if df is not None and not df.empty:
        df.columns = df.columns.str.strip()
        if 'SERIES' in df.columns:
            df['SERIES'] = df['SERIES'].astype(str).str.strip()
        if 'SYMBOL' in df.columns:
            df['SYMBOL'] = df['SYMBOL'].astype(str).str.strip()
        df = df[df['SERIES'] == 'EQ'].copy()
        df.to_csv(file_path, index=False)
        return df
    return None

def fetch_date_range(session, start_date, end_date):
    records = []
    current_date = start_date
    trading_days = 0
    while current_date <= end_date:
        if current_date.weekday() < 5:
            df = fetch_bhavcopy(session, current_date)
            if df is not None and not df.empty and 'CLOSE_PRICE' in df.columns:
                trading_days += 1
                date_formatted = current_date.strftime("%Y-%m-%d")
                # NEW: also keep PREV_CLOSE (needed for advance/decline)
                cols = ['SYMBOL', 'CLOSE_PRICE', 'PREV_CLOSE', 'DELIV_PER']
                for c in cols:
                    if c not in df.columns:
                        df[c] = float('nan')
                sub_df = df[cols].copy()
                sub_df['DATE'] = date_formatted
                for c in ['CLOSE_PRICE', 'PREV_CLOSE', 'DELIV_PER']:
                    sub_df[c] = pd.to_numeric(sub_df[c], errors='coerce')
                records.append(sub_df)
                print(f"✓ [{trading_days}] Fetched {date_formatted} ({len(sub_df)} stocks)")
                time.sleep(0.1)
            else:
                print(f"✗ No data for {current_date.strftime('%Y-%m-%d')}")
        current_date += timedelta(days=1)
    if not records:
        return None, None, None
    full_df = pd.concat(records, ignore_index=True)
    full_df = full_df.drop_duplicates(subset=['DATE', 'SYMBOL'], keep='last')
    price_matrix = full_df.pivot(index='DATE', columns='SYMBOL', values='CLOSE_PRICE')
    prev_matrix = full_df.pivot(index='DATE', columns='SYMBOL', values='PREV_CLOSE')
    deliv_matrix = full_df.pivot(index='DATE', columns='SYMBOL', values='DELIV_PER')
    return price_matrix, deliv_matrix, prev_matrix

# ==========================================
# Load or build matrices
# ==========================================
def build_matrices(start_date, end_date=None):
    if end_date is None:
        end_date = datetime.now()
    session = get_nse_session()
    print(f"Downloading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    price_matrix, deliv_matrix, prev_matrix = fetch_date_range(session, start_date, end_date)
    if price_matrix is None or price_matrix.empty:
        raise ValueError("No price data fetched.")
    price_matrix = price_matrix.sort_index()
    deliv_matrix = deliv_matrix.sort_index()
    prev_matrix = prev_matrix.sort_index()
    price_matrix.to_csv(PRICE_MATRIX_CSV)
    deliv_matrix.to_csv(DELIV_MATRIX_CSV)
    prev_matrix.to_csv(PREV_MATRIX_CSV)
    print(f"Price matrix saved to {PRICE_MATRIX_CSV} (shape: {price_matrix.shape})")
    print(f"DELIV matrix saved to {DELIV_MATRIX_CSV} (shape: {deliv_matrix.shape})")
    print(f"PREV_CLOSE matrix saved to {PREV_MATRIX_CSV} (shape: {prev_matrix.shape})")
    return price_matrix, deliv_matrix, prev_matrix

def load_matrices():
    paths = [PRICE_MATRIX_CSV, DELIV_MATRIX_CSV, PREV_MATRIX_CSV]
    if all(os.path.exists(p) for p in paths):
        print("Loading saved matrices...")
        price_df = pd.read_csv(PRICE_MATRIX_CSV, index_col='DATE', parse_dates=True)
        deliv_df = pd.read_csv(DELIV_MATRIX_CSV, index_col='DATE', parse_dates=True)
        prev_df = pd.read_csv(PREV_MATRIX_CSV, index_col='DATE', parse_dates=True)
        # allow ~10 days slack for weekends/holidays at the start
        if price_df.index.min() > DATA_START + timedelta(days=10):
            print("Existing matrices do not have enough history; re-downloading with wider range.")
            return build_matrices(DATA_START)
        common = price_df.index.intersection(deliv_df.index).intersection(prev_df.index)
        return price_df.loc[common], deliv_df.loc[common], prev_df.loc[common]
    # Missing files (e.g. first run with A/D) -> rebuild; cached bhavcopies make this fast
    return build_matrices(DATA_START)

# ==========================================
# NEW: Advance / Decline
# ==========================================
def compute_advance_decline(price_matrix, prev_matrix):
    """Advance = close > PREV_CLOSE, Decline = close < PREV_CLOSE.
    PREV_CLOSE is adjusted by NSE for splits/bonuses, so no distortion."""
    prev_matrix = prev_matrix.reindex(index=price_matrix.index, columns=price_matrix.columns)
    valid = price_matrix.notna() & prev_matrix.notna() & (prev_matrix > 0)
    adv = ((price_matrix > prev_matrix) & valid).sum(axis=1)
    dec = ((price_matrix < prev_matrix) & valid).sum(axis=1)
    total = valid.sum(axis=1)
    denom = total.where(total > 0)
    ad = pd.DataFrame({
        'adv': adv,                                        # number of stocks up
        'dec': dec,                                        # number of stocks down
        'unch': total - adv - dec,                         # number unchanged
        'adv_pct': 100 * adv / denom,                      # % of stocks up   (like bull_pct_p)
        'dec_pct': 100 * dec / denom,                      # % of stocks down (like bear_pct_p)
        'unch_pct': 100 * (total - adv - dec) / denom,     # % unchanged      (like side_pct_p)
    })
    return ad

# ==========================================
# Daily breadth: Price (trend), DELIV (trend), A/D
# ==========================================
def compute_daily_breadth(price_matrix, deliv_matrix, prev_matrix):
    ma_3m = price_matrix.rolling(window=MA_3M_DAYS, min_periods=30).mean()
    ma_6m = price_matrix.rolling(window=MA_6M_DAYS, min_periods=60).mean()
    ma_1y = price_matrix.rolling(window=MA_1Y_DAYS, min_periods=120).mean()

    ma_deliv_3m = deliv_matrix.rolling(window=MA_DELIV_3M_DAYS, min_periods=30).mean()
    ma_deliv_6m = deliv_matrix.rolling(window=MA_DELIV_6M_DAYS, min_periods=60).mean()
    ma_deliv_1y = deliv_matrix.rolling(window=MA_DELIV_1Y_DAYS, min_periods=120).mean()

    dates = price_matrix.index
    results = []
    for dt in dates:
        close = price_matrix.loc[dt]
        m3 = ma_3m.loc[dt]
        m6 = ma_6m.loc[dt]
        m1 = ma_1y.loc[dt]
        mask_p = close.notna() & m3.notna() & m6.notna() & m1.notna()
        if mask_p.sum() == 0:
            continue

        deliv = deliv_matrix.loc[dt]
        dm3 = ma_deliv_3m.loc[dt]
        dm6 = ma_deliv_6m.loc[dt]
        dm1 = ma_deliv_1y.loc[dt]
        mask_d = deliv.notna() & dm3.notna() & dm6.notna() & dm1.notna()

        close_p = close[mask_p]
        m3_p, m6_p, m1_p = m3[mask_p], m6[mask_p], m1[mask_p]

        above_all_p = (close_p > m3_p) & (close_p > m6_p) & (close_p > m1_p)
        below_all_p = (close_p < m3_p) & (close_p < m6_p) & (close_p < m1_p)
        bullish_p = above_all_p.sum()
        bearish_p = below_all_p.sum()
        sideways_p = len(close_p) - bullish_p - bearish_p
        total_p = len(close_p)

        if mask_d.sum() == 0:
            bullish_d = bearish_d = sideways_d = total_d = 0
        else:
            deliv_d = deliv[mask_d]
            dm3_d, dm6_d, dm1_d = dm3[mask_d], dm6[mask_d], dm1[mask_d]
            above_all_d = (deliv_d > dm3_d) & (deliv_d > dm6_d) & (deliv_d > dm1_d)
            below_all_d = (deliv_d < dm3_d) & (deliv_d < dm6_d) & (deliv_d < dm1_d)
            bullish_d = above_all_d.sum()
            bearish_d = below_all_d.sum()
            sideways_d = len(deliv_d) - bullish_d - bearish_d
            total_d = len(deliv_d)

        results.append({
            'date': dt,
            'bullish_p': bullish_p, 'bearish_p': bearish_p, 'sideways_p': sideways_p, 'total_p': total_p,
            'bull_pct_p': 100 * bullish_p / total_p if total_p > 0 else 0,
            'bear_pct_p': 100 * bearish_p / total_p if total_p > 0 else 0,
            'side_pct_p': 100 * sideways_p / total_p if total_p > 0 else 0,
            'bullish_d': bullish_d, 'bearish_d': bearish_d, 'sideways_d': sideways_d, 'total_d': total_d,
            'bull_pct_d': 100 * bullish_d / total_d if total_d > 0 else 0,
            'bear_pct_d': 100 * bearish_d / total_d if total_d > 0 else 0,
            'side_pct_d': 100 * sideways_d / total_d if total_d > 0 else 0,
        })
    breadth_df = pd.DataFrame(results).set_index('date')

    # NEW: join advance/decline 
    ad = compute_advance_decline(price_matrix, prev_matrix)
    breadth_df = breadth_df.join(ad, how='left')
    return breadth_df

# ==========================================
# Reports
# ==========================================
def generate_reports(breadth_df, start_date=None, end_date=None):
    breadth_df.index = pd.to_datetime(breadth_df.index)

    if start_date is not None:
        breadth_df = breadth_df[breadth_df.index >= pd.Timestamp(start_date)]
    if end_date is not None:
        breadth_df = breadth_df[breadth_df.index <= pd.Timestamp(end_date)]

    monthly = breadth_df.resample('ME').agg({
        'bullish_p': 'mean', 'bearish_p': 'mean', 'sideways_p': 'mean', 'total_p': 'mean',
        'bull_pct_p': 'mean', 'bear_pct_p': 'mean', 'side_pct_p': 'mean',
        'bullish_d': 'mean', 'bearish_d': 'mean', 'sideways_d': 'mean', 'total_d': 'mean',
        'bull_pct_d': 'mean', 'bear_pct_d': 'mean', 'side_pct_d': 'mean',
        # A/D (monthly = average of daily values)
        'adv': 'mean', 'dec': 'mean', 'unch': 'mean',
        'adv_pct': 'mean', 'dec_pct': 'mean', 'unch_pct': 'mean',
    }).round(2)

    monthly.index = monthly.index.strftime('%b-%y')
    monthly.to_csv(MONTHLY_REPORT_CSV)
    print(f"\nMonthly report saved to {MONTHLY_REPORT_CSV}")

    daily = breadth_df.round(2)
    daily.to_csv(DAILY_REPORT_CSV)
    print(f"Daily report saved to {DAILY_REPORT_CSV}")

    print("\n" + "=" * 100)
    print("  MONTHLY BREADTH REPORT (average per trading day)")
    print("=" * 100)

    print("\n--- Price Trend Breadth ---")
    price_table = monthly[['bullish_p', 'bearish_p', 'sideways_p', 'total_p',
                           'bull_pct_p', 'bear_pct_p', 'side_pct_p']].copy()
    price_table.columns = ['Avg Bull', 'Avg Bear', 'Avg Side', 'Avg Total', 'Bull %', 'Bear %', 'Side %']
    print(price_table.to_string())

    print("\n--- DELIV % Breadth ---")
    deliv_table = monthly[['bullish_d', 'bearish_d', 'sideways_d', 'total_d',
                           'bull_pct_d', 'bear_pct_d', 'side_pct_d']].copy()
    deliv_table.columns = ['Avg Bull', 'Avg Bear', 'Avg Side', 'Avg Total', 'Bull %', 'Bear %', 'Side %']
    print(deliv_table.to_string())

    print("\n--- Advance / Decline ---")
    ad_table = monthly[['adv', 'dec', 'unch', 'adv_pct', 'dec_pct', 'unch_pct']].copy()
    ad_table.columns = ['Avg Adv', 'Avg Dec', 'Avg Unch', 'Adv %', 'Dec %', 'Unch %']
    print(ad_table.to_string())

    print("=" * 100)
    print("\nDaily report preview (first 5 rows):")
    print(daily.head().to_string())
    print("(Full daily data is in the CSV file.)")
    return monthly, daily

# ==========================================
# Main
# ==========================================
if __name__ == "__main__":
    print(f"Report period: {REPORT_START.strftime('%Y-%m-%d')} to {REPORT_END.strftime('%Y-%m-%d')}")
    print(f"Data warm-up starts from {DATA_START.strftime('%Y-%m-%d')}\n")

    price_matrix, deliv_matrix, prev_matrix = load_matrices()
    print(f"Price matrix shape: {price_matrix.shape}")
    print(f"Date range: {price_matrix.index.min()} to {price_matrix.index.max()}")

    print("\nComputing daily breadth (Price trend, DELIV trend, Advance/Decline)...")
    breadth_df = compute_daily_breadth(price_matrix, deliv_matrix, prev_matrix)
    print(f"Breadth data from {breadth_df.index.min()} to {breadth_df.index.max()}")

    monthly, daily = generate_reports(breadth_df, REPORT_START, REPORT_END)
    print(f"\nDone. Monthly report has {len(monthly)} months.")
