import os
import io
import time
from datetime import datetime, timedelta
import pandas as pd
import requests

# ==========================================
# CONFIGURATION & OUTPUT DIRECTORIES
# ==========================================
CACHE_DIR = "nse_bhavcopy_cache"
OUTPUT_DIR = "nse_breadth_outputs"

# CSV Output File Paths
RAW_PRICES_CSV = os.path.join(OUTPUT_DIR, "nse_1year_raw_close_prices.csv")
SUMMARY_ANALYSIS_CSV = os.path.join(OUTPUT_DIR, "nse_stock_breadth_analysis.csv")

# Moving Average periods (in trading days)
MA_3M_DAYS = 60    # ~3 Months
MA_6M_DAYS = 120   # ~6 Months
MA_1Y_DAYS = 250   # ~1 Year

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/all-reports",
}

# Create output directories
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==========================================
# 1. NSE SESSION & FETCHING LOGIC
# ==========================================
def get_nse_session():
    """Initializes a requests session with NSE cookies."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=10)
    except Exception as e:
        print(f"Warning: Failed to initialize NSE session cookies: {e}")
    return session

def fetch_bhavcopy(session, date_obj):
    """
    Downloads Bhavcopy CSV for a specific date from NSE.
    Returns DataFrame or None if market was closed / holiday.
    """
    date_str_api = date_obj.strftime("%d-%b-%Y") # e.g., '12-Aug-2026'
    date_str_archive = date_obj.strftime("%d%m%Y") # e.g., '12082026'
    
    file_path = os.path.join(CACHE_DIR, f"sec_bhavdata_full_{date_str_archive}.csv")
    
    # Check if cached locally
    if os.path.exists(file_path):
        try:
            return pd.read_csv(file_path)
        except Exception:
            pass

    # Try NSE Reports API Endpoint
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

    # Fallback to Direct Archives Link if API fails
    if df is None:
        archive_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str_archive}.csv"
        try:
            resp = session.get(archive_url, timeout=8)
            if resp.status_code == 200 and "SYMBOL" in resp.text:
                df = pd.read_csv(io.StringIO(resp.text))
        except Exception:
            df = None

    # Save to cache
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
    """Utility function to download daily data for a specific date range."""
    records = []
    current_date = start_date
    trading_days = 0

    while current_date <= end_date:
        if current_date.weekday() < 5: # Skip weekends
            df = fetch_bhavcopy(session, current_date)
            if df is not None and not df.empty and 'CLOSE_PRICE' in df.columns:
                trading_days += 1
                date_formatted = current_date.strftime("%Y-%m-%d")
                
                sub_df = df[['SYMBOL', 'CLOSE_PRICE']].copy()
                sub_df['DATE'] = date_formatted
                sub_df['CLOSE_PRICE'] = pd.to_numeric(sub_df['CLOSE_PRICE'], errors='coerce')
                records.append(sub_df)
                
                print(f"✓ [{trading_days}] Fetched {date_formatted} ({len(sub_df)} stocks)")
                time.sleep(0.1)
            else:
                print(f"✗ Market Closed / No Data for {current_date.strftime('%Y-%m-%d')}")
                
        current_date += timedelta(days=1)

    if not records:
        return None

    full_df = pd.concat(records, ignore_index=True)
    price_matrix = full_df.pivot(index='DATE', columns='SYMBOL', values='CLOSE_PRICE')
    return price_matrix


# ==========================================
# 2. INCREMENTAL DATA ENGINE
# ==========================================
def get_updated_price_matrix():
    """
    Checks existing CSV.
    - If CSV exists: Downloads ONLY missing incremental days.
    - If CSV missing: Performs FULL 1-year download.
    """
    session = get_nse_session()
    today = datetime.now()
    start_date_full = today - timedelta(days=365)

    existing_df = None

    # Check if raw price CSV already exists
    if os.path.exists(RAW_PRICES_CSV):
        try:
            existing_df = pd.read_csv(RAW_PRICES_CSV, index_col='DATE')
            existing_df.index = pd.to_datetime(existing_df.index)
            existing_df = existing_df.sort_index()
            print(f"[FOUND EXISTING DATA] Latest date in CSV: {existing_df.index.max().strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Warning: Could not load existing CSV ({e}). Falling back to full download.")
            existing_df = None

    if existing_df is not None and not existing_df.empty:
        last_recorded_date = existing_df.index.max()
        fetch_start_date = last_recorded_date + timedelta(days=1)

        # If data is already up to date
        if fetch_start_date.date() > today.date():
            print("🚀 Data is already fully up to date!")
            existing_df.index = existing_df.index.strftime("%Y-%m-%d")
            return existing_df

        print(f"🔄 INCREMENTAL MODE: Downloading missing data from {fetch_start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}...")
        new_price_matrix = fetch_date_range(session, fetch_start_date, today)

        if new_price_matrix is not None and not new_price_matrix.empty:
            new_price_matrix.index = pd.to_datetime(new_price_matrix.index)
            
            # Combine old and new records
            combined_df = pd.concat([existing_df, new_price_matrix], axis=0)
            # Remove duplicated dates (keep newest)
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')].sort_index()
            
            # Keep rolling 1-year window (~365 days)
            cutoff_date = today - timedelta(days=365)
            combined_df = combined_df[combined_df.index >= cutoff_date]
            
            combined_df.index = combined_df.index.strftime("%Y-%m-%d")
            combined_df.to_csv(RAW_PRICES_CSV)
            print(f"✅ [INCREMENTAL UPDATE COMPLETE] Appended new rows and saved to '{RAW_PRICES_CSV}'.")
            return combined_df
        else:
            print("ℹ️ No new trading days were available to append. Using existing data.")
            existing_df.index = existing_df.index.strftime("%Y-%m-%d")
            return existing_df

    # FULL DOWNLOAD FALLBACK
    print(f"⚡ FULL DOWNLOAD MODE: No existing data found. Fetching full 365 days from {start_date_full.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}...")
    full_price_matrix = fetch_date_range(session, start_date_full, today)

    if full_price_matrix is None or full_price_matrix.empty:
        raise ValueError("Failed to fetch any data from NSE servers.")

    full_price_matrix = full_price_matrix.sort_index().ffill()
    full_price_matrix.to_csv(RAW_PRICES_CSV)
    print(f"✅ [FULL DOWNLOAD COMPLETE] Full price matrix saved to '{RAW_PRICES_CSV}'.")
    return full_price_matrix


# ==========================================
# 3. BREADTH ANALYSIS & ANALYSIS CSV SAVE
# ==========================================
def analyze_and_save_results(price_matrix):
    """Calculates 3M, 6M, 1Y moving averages and saves classification results to CSV."""
    ma_3m = price_matrix.rolling(window=MA_3M_DAYS, min_periods=30).mean()
    ma_6m = price_matrix.rolling(window=MA_6M_DAYS, min_periods=60).mean()
    ma_1y = price_matrix.rolling(window=MA_1Y_DAYS, min_periods=120).mean()

    latest_date = price_matrix.index[-1]
    
    summary_df = pd.DataFrame({
        'Close_Price': price_matrix.loc[latest_date],
        'MA_3M_60D': ma_3m.loc[latest_date],
        'MA_6M_120D': ma_6m.loc[latest_date],
        'MA_1Y_250D': ma_1y.loc[latest_date]
    }).dropna()

    # Classification logic
    cond_above_all = (
        (summary_df['Close_Price'] > summary_df['MA_3M_60D']) &
        (summary_df['Close_Price'] > summary_df['MA_6M_120D']) &
        (summary_df['Close_Price'] > summary_df['MA_1Y_250D'])
    )

    cond_below_all = (
        (summary_df['Close_Price'] < summary_df['MA_3M_60D']) &
        (summary_df['Close_Price'] < summary_df['MA_6M_120D']) &
        (summary_df['Close_Price'] < summary_df['MA_1Y_250D'])
    )

    summary_df['Status'] = 'Sideways / Mixed'
    summary_df.loc[cond_above_all, 'Status'] = 'Above All (Bullish Alignment)'
    summary_df.loc[cond_below_all, 'Status'] = 'Below All (Bearish Alignment)'

    # SAVE ANALYSIS CSV
    summary_df.to_csv(SUMMARY_ANALYSIS_CSV)
    print(f"\n[ANALYSIS SAVED] Detailed stock classification saved to: '{SUMMARY_ANALYSIS_CSV}'")

    # Display Breakdown
    total = len(summary_df)
    counts = summary_df['Status'].value_counts()
    percentages = (counts / total * 100).round(2)

    print("\n" + "="*60)
    print(f"  NSE MARKET BREADTH SUMMARY (As of {latest_date})")
    print("="*60)
    print(pd.DataFrame({'Stock Count': counts, 'Percentage (%)': percentages}).to_string())
    print("="*60)


if __name__ == "__main__":
    price_matrix = get_updated_price_matrix()
    analyze_and_save_results(price_matrix)