import requests
import pandas as pd
import time

BASE_URL = "https://www.nseindia.com/api/historicalOR/advances-decline-monthly"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
}

session = requests.Session()
session.headers.update(headers)

# First visit NSE to establish cookies/session
session.get("https://www.nseindia.com/", timeout=20)

all_data = []

for year in range(1990, 2027):
    url = BASE_URL

    try:
        response = session.get(
            url,
            params={"year": year},
            timeout=20
        )

        response.raise_for_status()

        result = response.json()
        data = result.get("data", [])

        print(f"{year}: {len(data)} records")

        all_data.extend(data)

        # Be polite to NSE
        time.sleep(1)

    except Exception as e:
        print(f"{year}: ERROR - {e}")

# Convert to DataFrame
df = pd.DataFrame(all_data)

# Remove duplicates just in case
df = df.drop_duplicates()

# Convert timestamp to datetime
df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")

# Sort chronologically
df = df.sort_values("TIMESTAMP")

# Save
df.to_csv("nse_advances_declines_1990_2026.csv", index=False)
df.to_json(
    "nse_advances_declines_1990_2026.json",
    orient="records",
    date_format="iso",
    indent=2
)

print("\nTotal records:", len(df))
print(df.head())
print(df.tail())