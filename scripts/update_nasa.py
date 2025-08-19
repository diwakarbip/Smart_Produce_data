# update_nasa.py
import requests
import pandas as pd
from datetime import date, timedelta
import os

CSV_FILE = "data/nasa_power_data.csv"

LAT, LON = 38.57, -7.91  # Palmela, PT

# ✅ Only valid daily parameters
PARAMS = [
    "T2M",              # Temperature at 2m
    "RH2M",             # Relative Humidity at 2m
    "WS2M",             # Wind Speed at 2m
    "WD2M",             # Wind Direction at 2m
    "PRECTOTCORR",      # Precipitation (mm/day)
    "ALLSKY_SFC_SW_DWN" # Surface solar radiation (W/m²)
]

def fetch_data(start, end):
    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        f"?parameters={','.join(PARAMS)}"
        f"&community=AG"
        f"&longitude={LON}&latitude={LAT}"
        f"&start={start}&end={end}&format=JSON"
    )
    print(f"Fetching NASA POWER {start} → {end}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    data = r.json()["properties"]["parameter"]
    df = pd.DataFrame({k: v for k, v in data.items()})
    df.index = pd.to_datetime(df.index, format="%Y%m%d")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "datetime"}, inplace=True)
    return df

def main():
    today = date.today()
    end_date = (today - timedelta(days=5)).strftime("%Y%m%d")  # NASA lag buffer
    start_date = "20240101"

    if os.path.exists(CSV_FILE):
        df_old = pd.read_csv(CSV_FILE, parse_dates=["datetime"])
        last_date = df_old["datetime"].max().date()
        start_date = (last_date + timedelta(days=1)).strftime("%Y%m%d")
    else:
        df_old = pd.DataFrame()

    if start_date > end_date:
        print("No new data available from NASA POWER.")
        return

    # Fetch in 6-month chunks
    all_new = []
    cur = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    while cur <= end:
        chunk_end = min(cur + pd.DateOffset(months=6) - pd.Timedelta(days=1), end)
        try:
            df_chunk = fetch_data(cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"))
            all_new.append(df_chunk)
        except Exception as e:
            print(f"Skipping {cur.date()} to {chunk_end.date()} → {e}")
        cur = chunk_end + timedelta(days=1)

    if all_new:
        df_new = pd.concat(all_new, ignore_index=True)

        # Rename columns
        df_new = df_new.rename(columns={
            "T2M": "temperature",
            "RH2M": "humidity",
            "WS2M": "wind_speed",
            "WD2M": "wind_direction",
            "PRECTOTCORR": "precipitation",
            "ALLSKY_SFC_SW_DWN": "radiation",
        })
        df_new["source"] = "nasa_power"

        df_all = pd.concat([df_old, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=["datetime"], keep="last", inplace=True)
        os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
        df_all.to_csv(CSV_FILE, index=False)
        print(f"✅ Updated {CSV_FILE} with {len(df_new)} new rows.")
    else:
        print("⚠️ No new NASA POWER data fetched.")

if __name__ == "__main__":
    main()
