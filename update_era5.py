# update_era5.py
import cdsapi
import pandas as pd
from datetime import date, timedelta
import os
import xarray as xr
import tempfile
import shutil

CSV_FILE = "era5_data.csv"

c = cdsapi.Client()

PARAMS = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "surface_pressure",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "total_precipitation",
    "surface_solar_radiation_downwards",
]

# Palmela coordinates (small bounding box to avoid CDS error)
LAT, LON = 38.57, -7.91
AREA = [LAT + 0.05, LON - 0.05, LAT - 0.05, LON + 0.05]  # N,W,S,E

def fetch_chunk(start, end):
    print(f"Fetching ERA5 data {start} → {end}")
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)
    days = [d.strftime("%d") for d in pd.date_range(start_date, end_date)]

    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    temp_filename = os.path.join(temp_dir, "era5_data.nc")

    try:
        # Download the data
        c.retrieve(
    "reanalysis-era5-single-levels",
    {
        "product_type": "reanalysis",
        "variable": [
            "2m_temperature",
            "2m_dewpoint_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "surface_solar_radiation_downwards",
            "total_precipitation",
        ],
        "year": "2024",
        "month": "01",
        "day": [f"{d:02d}" for d in range(1, 32)],
        "time": [f"{h:02d}:00" for h in range(24)],
        "format": "netcdf",
    },
    "era5_data.nc",
)


        # Verify file
        if not os.path.exists(temp_filename):
            raise ValueError("Downloaded file not found")

        file_size = os.path.getsize(temp_filename)
        print(f"Downloaded file size: {file_size} bytes")

        if file_size < 10000:
            with open(temp_filename, "rb") as f:
                head = f.read(200)
            raise ValueError(f"File too small ({file_size} bytes). Content:\n{head}")

        # Open dataset
        ds = xr.open_dataset(temp_filename, engine="netcdf4")

        # Convert to DataFrame and select nearest grid point
        df = ds.to_dataframe().reset_index()
        ds.close()

        # Pick nearest lat/lon to Palmela
        df = df.loc[
            (df["latitude"].sub(LAT).abs().idxmin()) :
            (df["longitude"].sub(LON).abs().idxmin())
        ]

        # Rename columns
        df = df.rename(
            columns={
                "2m_temperature": "temperature",
                "2m_dewpoint_temperature": "dewpoint",
                "surface_pressure": "pressure",
                "10m_u_component_of_wind": "u_wind",
                "10m_v_component_of_wind": "v_wind",
                "total_precipitation": "precipitation",
                "surface_solar_radiation_downwards": "radiation",
            }
        )

        # Calculate wind speed
        df["wind_speed"] = (df["u_wind"] ** 2 + df["v_wind"] ** 2) ** 0.5
        df["source"] = "era5"

        return df[
            [
                "time",
                "temperature",
                "dewpoint",
                "pressure",
                "wind_speed",
                "precipitation",
                "radiation",
                "source",
            ]
        ]

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    today = date.today()
    end_date = today - timedelta(days=3)
    start_date = date(2024, 1, 1)

    # Load old CSV if exists
    if os.path.exists(CSV_FILE):
        df_old = pd.read_csv(CSV_FILE, parse_dates=["time"])
        last_date = df_old["time"].max().date()
        start_date = last_date + timedelta(days=1)
    else:
        df_old = pd.DataFrame()

    all_new = []
    cur = start_date

    # Loop in 30-day chunks
    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=30), end_date)
        try:
            df_chunk = fetch_chunk(
                cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
            )
            all_new.append(df_chunk)
            cur = chunk_end + timedelta(days=1)
        except Exception as e:
            print(f"Failed to fetch data for {cur} to {chunk_end}: {e}")
            break

    # Combine old and new data
    if all_new:
        df_new = pd.concat(all_new, ignore_index=True)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=["time"], inplace=True)
        df_all.to_csv(CSV_FILE, index=False)
        print(f"Updated {CSV_FILE} with {len(df_new)} new rows.")
    else:
        print("No new ERA5 data available.")


if __name__ == "__main__":
    main()
