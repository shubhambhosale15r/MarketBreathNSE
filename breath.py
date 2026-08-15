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

# Moving average periods for DELIV_PER (same as price, but you can edit separately)
MA_DELIV_3M_DAYS = 60
MA_DELIV_6M_DAYS = 120
MA_DELIV_1Y_DAYS = 252

PRICE_MATRIX_CSV = "nse_breadth_outputs/nse_full_price_matrix.csv"
CACHE_DIR = "nse_bhavcopy_cache"

MONTHLY_REPORT_CSV = "nse_breadth_outputs/monthly_breadth_report.csv"
DAILY_REPORT_CSV = "nse_breadth_outputs/daily_breadth_report.csv"

REPORT_START = datetime.now() - timedelta(days=365)
REPORT_END = datetime.now()

WARMUP_DAYS = 450
DATA_START = REPORT_START - timedelta(days=WARMUP_DAYS)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/all-reports",
}

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs("nse_breadth_outputs", exist_ok=True)

# ==========================================
# NSE BHAVCOPY FETCHING (now includes DELIV_PER)
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
            return pd.read_csv(file_path)
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
                # Keep CLOSE_PRICE and DELIV_PER
                sub_df = df[['SYMBOL', 'CLOSE_PRICE', 'DELIV_PER']].copy()
                sub_df['DATE'] = date_formatted
                sub_df['CLOSE_PRICE'] = pd.to_numeric(sub_df['CLOSE_PRICE'], errors='coerce')
                sub_df['DELIV_PER'] = pd.to_numeric(sub_df['DELIV_PER'], errors='coerce')
                records.append(sub_df)
                print(f"✓ [{trading_days}] Fetched {date_formatted} ({len(sub_df)} stocks)")
                time.sleep(0.1)
            else:
                print(f"✗ No data for {current_date.strftime('%Y-%m-%d')}")
        current_date += timedelta(days=1)
    if not records:
        return None
    full_df = pd.concat(records, ignore_index=True)
    # Pivot for both CLOSE_PRICE and DELIV_PER separately
    price_matrix = full_df.pivot(index='DATE', columns='SYMBOL', values='CLOSE_PRICE')
    deliv_matrix = full_df.pivot(index='DATE', columns='SYMBOL', values='DELIV_PER')
    return price_matrix, deliv_matrix

# ==========================================
# Load or build price & DELIV matrices
# ==========================================
def build_matrices(start_date, end_date=None):
    if end_date is None:
        end_date = datetime.now()
    session = get_nse_session()
    print(f"Downloading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    price_matrix, deliv_matrix = fetch_date_range(session, start_date, end_date)
    if price_matrix is None or price_matrix.empty:
        raise ValueError("No price data fetched.")
    if deliv_matrix is None:
        deliv_matrix = pd.DataFrame(index=price_matrix.index, columns=price_matrix.columns)
    price_matrix = price_matrix.sort_index()
    deliv_matrix = deliv_matrix.sort_index()
    # Save both
    price_matrix.to_csv(PRICE_MATRIX_CSV)
    deliv_matrix.to_csv("nse_breadth_outputs/nse_full_deliv_matrix.csv")
    print(f"Price matrix saved to {PRICE_MATRIX_CSV} (shape: {price_matrix.shape})")
    print(f"DELIV matrix saved to nse_breadth_outputs/nse_full_deliv_matrix.csv (shape: {deliv_matrix.shape})")
    return price_matrix, deliv_matrix

def load_matrices():
    price_path = PRICE_MATRIX_CSV
    deliv_path = "nse_breadth_outputs/nse_full_deliv_matrix.csv"
    if os.path.exists(price_path) and os.path.exists(deliv_path):
        print(f"Loading price matrix from {price_path}")
        price_df = pd.read_csv(price_path, index_col='DATE', parse_dates=True)
        print(f"Loading DELIV matrix from {deliv_path}")
        deliv_df = pd.read_csv(deliv_path, index_col='DATE', parse_dates=True)
        if not isinstance(price_df.index, pd.DatetimeIndex):
            price_df.index = pd.to_datetime(price_df.index)
            deliv_df.index = pd.to_datetime(deliv_df.index)
        first_date = price_df.index.min()
        if first_date > DATA_START:
            print("Existing matrices do not have enough history; re-downloading with wider range.")
            return build_matrices(DATA_START)
        # Align indices
        common_dates = price_df.index.intersection(deliv_df.index)
        price_df = price_df.loc[common_dates]
        deliv_df = deliv_df.loc[common_dates]
        return price_df, deliv_df
    else:
        return build_matrices(DATA_START)

# ==========================================
# Compute daily breadth for BOTH price and DELIV
# ==========================================
def compute_daily_breadth(price_matrix, deliv_matrix):
    # Price MAs
    ma_3m = price_matrix.rolling(window=MA_3M_DAYS, min_periods=30).mean()
    ma_6m = price_matrix.rolling(window=MA_6M_DAYS, min_periods=60).mean()
    ma_1y = price_matrix.rolling(window=MA_1Y_DAYS, min_periods=120).mean()

    # DELIV MAs
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
        # Price mask
        mask_p = close.notna() & m3.notna() & m6.notna() & m1.notna()
        if mask_p.sum() == 0:
            continue
        
        # DELIV data for the same date
        deliv = deliv_matrix.loc[dt]
        dm3 = ma_deliv_3m.loc[dt]
        dm6 = ma_deliv_6m.loc[dt]
        dm1 = ma_deliv_1y.loc[dt]
        mask_d = deliv.notna() & dm3.notna() & dm6.notna() & dm1.notna()
        
        # For price breadth, use mask_p
        close_p = close[mask_p]
        m3_p = m3[mask_p]
        m6_p = m6[mask_p]
        m1_p = m1[mask_p]

        above_all_p = (close_p > m3_p) & (close_p > m6_p) & (close_p > m1_p)
        below_all_p = (close_p < m3_p) & (close_p < m6_p) & (close_p < m1_p)
        bullish_p = above_all_p.sum()
        bearish_p = below_all_p.sum()
        sideways_p = len(close_p) - bullish_p - bearish_p
        total_p = len(close_p)

        # For DELIV breadth, use mask_d (different stocks may have valid DELIV)
        if mask_d.sum() == 0:
            # No DELIV data for any stock on this day
            bullish_d = 0
            bearish_d = 0
            sideways_d = 0
            total_d = 0
        else:
            deliv_d = deliv[mask_d]
            dm3_d = dm3[mask_d]
            dm6_d = dm6[mask_d]
            dm1_d = dm1[mask_d]
            above_all_d = (deliv_d > dm3_d) & (deliv_d > dm6_d) & (deliv_d > dm1_d)
            below_all_d = (deliv_d < dm3_d) & (deliv_d < dm6_d) & (deliv_d < dm1_d)
            bullish_d = above_all_d.sum()
            bearish_d = below_all_d.sum()
            sideways_d = len(deliv_d) - bullish_d - bearish_d
            total_d = len(deliv_d)

        results.append({
            'date': dt,
            # Price
            'bullish_p': bullish_p,
            'bearish_p': bearish_p,
            'sideways_p': sideways_p,
            'total_p': total_p,
            'bull_pct_p': 100 * bullish_p / total_p if total_p > 0 else 0,
            'bear_pct_p': 100 * bearish_p / total_p if total_p > 0 else 0,
            'side_pct_p': 100 * sideways_p / total_p if total_p > 0 else 0,
            # DELIV
            'bullish_d': bullish_d,
            'bearish_d': bearish_d,
            'sideways_d': sideways_d,
            'total_d': total_d,
            'bull_pct_d': 100 * bullish_d / total_d if total_d > 0 else 0,
            'bear_pct_d': 100 * bearish_d / total_d if total_d > 0 else 0,
            'side_pct_d': 100 * sideways_d / total_d if total_d > 0 else 0,
        })
    breadth_df = pd.DataFrame(results).set_index('date')
    return breadth_df

# ==========================================
# Generate reports (now with DELIV)
# ==========================================
def generate_reports(breadth_df, start_date=None, end_date=None):
    breadth_df.index = pd.to_datetime(breadth_df.index)

    if start_date is not None:
        breadth_df = breadth_df[breadth_df.index >= pd.Timestamp(start_date)]
    if end_date is not None:
        breadth_df = breadth_df[breadth_df.index <= pd.Timestamp(end_date)]

    # Monthly aggregation
    monthly = breadth_df.resample('ME').agg({
        # Price
        'bullish_p': 'mean', 'bearish_p': 'mean', 'sideways_p': 'mean', 'total_p': 'mean',
        'bull_pct_p': 'mean', 'bear_pct_p': 'mean', 'side_pct_p': 'mean',
        # DELIV
        'bullish_d': 'mean', 'bearish_d': 'mean', 'sideways_d': 'mean', 'total_d': 'mean',
        'bull_pct_d': 'mean', 'bear_pct_d': 'mean', 'side_pct_d': 'mean',
    }).round(2)

    monthly.index = monthly.index.strftime('%b-%y')

    monthly.to_csv(MONTHLY_REPORT_CSV)
    print(f"\nMonthly report saved to {MONTHLY_REPORT_CSV}")

    # Daily report
    daily = breadth_df.round(2)
    daily.to_csv(DAILY_REPORT_CSV)
    print(f"Daily report saved to {DAILY_REPORT_CSV}")

    # --- Print terminal report ---
    print("\n" + "="*100)
    print("  MONTHLY BREADTH REPORT (average per trading day)")
    print("="*100)

    # Price section
    print("\n--- Price Breadth ---")
    price_cols = ['bullish_p', 'bearish_p', 'sideways_p', 'total_p', 'bull_pct_p', 'bear_pct_p', 'side_pct_p']
    price_table = monthly[price_cols]
    price_table.columns = ['Avg Bull', 'Avg Bear', 'Avg Side', 'Avg Total', 'Bull %', 'Bear %', 'Side %']
    print(price_table.to_string())

    # DELIV section
    print("\n--- DELIV % Breadth ---")
    deliv_cols = ['bullish_d', 'bearish_d', 'sideways_d', 'total_d', 'bull_pct_d', 'bear_pct_d', 'side_pct_d']
    deliv_table = monthly[deliv_cols]
    deliv_table.columns = ['Avg Bull', 'Avg Bear', 'Avg Side', 'Avg Total', 'Bull %', 'Bear %', 'Side %']
    print(deliv_table.to_string())

    print("="*100)

    # Preview daily
    print("\nDaily report preview (first 5 rows):")
    print(daily.head().to_string())
    print("(Full daily data is in the CSV file.)")

    return monthly, daily

# ==========================================
# Main
# ==========================================
if __name__ == "__main__":
    print(f"Report period: {REPORT_START.strftime('%Y-%m-%d')} to {REPORT_END.strftime('%Y-%m-%d')}")
    print(f"Data warm‑up starts from {DATA_START.strftime('%Y-%m-%d')} (to ensure MA availability)\n")

    print("Loading (or downloading) price and DELIV matrices...")
    price_matrix, deliv_matrix = load_matrices()
    print(f"Price matrix shape: {price_matrix.shape}")
    print(f"DELIV matrix shape: {deliv_matrix.shape}")
    print(f"Date range: {price_matrix.index.min()} to {price_matrix.index.max()}")

    print("\nComputing daily breadth for both Price and DELIV %...")
    breadth_df = compute_daily_breadth(price_matrix, deliv_matrix)
    print(f"Breadth data from {breadth_df.index.min()} to {breadth_df.index.max()}")

    print("\nGenerating reports...")
    monthly, daily = generate_reports(breadth_df, REPORT_START, REPORT_END)

    print(f"\nDone. Monthly report has {len(monthly)} months.")