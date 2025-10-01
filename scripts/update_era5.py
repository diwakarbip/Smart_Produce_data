# update_era5.py
import os
import tarfile
import zipfile
import shutil
import tempfile
from datetime import date, timedelta

import numpy as np
import pandas as pd
import xarray as xr
import cdsapi

# always save into Smart_Produce/data/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_FILE = os.path.join(BASE_DIR, "data", "era5_data.csv")

PARAMS = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "surface_pressure",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "total_precipitation",
    "surface_solar_radiation_downwards",
]

# Palmela coordinates
LAT, LON = 38.57, -7.91
AREA = [LAT + 0.1, LON - 0.1, LAT - 0.1, LON + 0.1]

DAILY_AGG = {
    "temperature": "mean",
    "dewpoint": "mean",
    "pressure": "mean",
    "wind_speed": "mean",
    "precipitation": "sum",
    "radiation": "sum",
}

c = cdsapi.Client()

def _log_head(path, max_bytes=400):
    try:
        with open(path, "rb") as f:
            head = f.read(max_bytes)
        print(f"[DEBUG] File head ({len(head)} bytes): {head[:max_bytes]!r}")
    except Exception as e:
        print(f"[DEBUG] Could not read head of file {path}: {e}")

def _open_any_netcdf(path):
    if tarfile.is_tarfile(path):
        with tarfile.open(path) as tf, tempfile.TemporaryDirectory() as tdir:
            members = [m for m in tf.getmembers() if m.name.lower().endswith(".nc")]
            if not members:
                _log_head(path)
                raise ValueError("Downloaded TAR has no .nc inside.")
            tf.extract(members[0], path=tdir)
            return _open_any_netcdf(os.path.join(tdir, members[0].name))
    
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf, tempfile.TemporaryDirectory() as tdir:
            nc_members = [n for n in zf.namelist() if n.lower().endswith(".nc")]
            if not nc_members:
                _log_head(path)
                raise ValueError("Downloaded ZIP has no .nc inside.")
            zf.extract(nc_members[0], path=tdir)
            return _open_any_netcdf(os.path.join(tdir, nc_members[0]))
    
    last_err = None
    for eng in ("netcdf4", "h5netcdf"):
        try:
            return xr.open_dataset(path, engine=eng)
        except Exception as e:
            last_err = e
            print(f"[DEBUG] xarray engine '{eng}' failed: {e}")
    _log_head(path)
    raise ValueError(f"Could not open NetCDF: {last_err}")

def _select_nearest_point(ds, lat, lon):
    lat_name = "latitude" if "latitude" in ds.coords else ("lat" if "lat" in ds.coords else None)
    lon_name = "longitude" if "longitude" in ds.coords else ("lon" if "lon" in ds.coords else None)
    if lat_name is None or lon_name is None:
        raise ValueError(f"Dataset missing lat/lon coords. Found coords: {list(ds.coords)}")
    lat_vals = ds[lat_name].values
    lon_vals = ds[lon_name].values
    lon360 = lon % 360
    lon_idx = int(np.nanargmin(np.minimum(np.abs(lon_vals - lon), np.abs(lon_vals - lon360))))
    lat_idx = int(np.nanargmin(np.abs(lat_vals - lat)))
    ds_point = ds.isel({lat_name: lat_idx, lon_name: lon_idx})
    if "expver" in ds_point.dims:
        ds_point = ds_point.mean("expver")
    return ds_point

def _rename_columns(df):
    mapping = {
        "2m_temperature": "temperature",
        "2m_dewpoint_temperature": "dewpoint",
        "surface_pressure": "pressure",
        "10m_u_component_of_wind": "u_wind",
        "10m_v_component_of_wind": "v_wind",
        "total_precipitation": "precipitation",
        "surface_solar_radiation_downwards": "radiation",
       
        "t2m": "temperature",
        "d2m": "dewpoint",
        "sp": "pressure",
        "u10": "u_wind",
        "v10": "v_wind",
        "tp": "precipitation",
        "ssrd": "radiation",
    }
    return df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

def _find_time_col(cols):
    for c in cols:
        lc = str(c).lower()
        if lc in ("time", "valid_time", "datetime", "date_time"):
            return c
    return None

def _process_downloaded_nc(nc_path) -> pd.DataFrame:
    ds = _open_any_netcdf(nc_path)
    ds_point = _select_nearest_point(ds, LAT, LON)

    df = ds_point.to_dataframe().reset_index()
    df = _rename_columns(df)

    time_col = _find_time_col(df.columns)
    if time_col is None:
        raise ValueError(f"No time coordinate found in dataset columns: {df.columns.tolist()}")
    if time_col != "time":
        df = df.rename(columns={time_col: "time"})

    if {"u_wind", "v_wind"}.issubset(df.columns):
        df["wind_speed"] = np.sqrt(df["u_wind"] ** 2 + df["v_wind"] ** 2)

    desired = ["time", "temperature", "dewpoint", "pressure", "wind_speed",
               "precipitation", "radiation"]
    keep = [c for c in desired if c in df.columns]
    df = df[keep]

    df["date"] = pd.to_datetime(df["time"]).dt.normalize()
    agg = {k: DAILY_AGG[k] for k in DAILY_AGG if k in df.columns}
    df_daily = df.groupby("date").agg(agg).reset_index()

    if "temperature" in df_daily.columns:
        df_daily["temperature"] = df_daily["temperature"] - 273.15  
    if "dewpoint" in df_daily.columns:
        df_daily["dewpoint"] = df_daily["dewpoint"] - 273.15        
    if "pressure" in df_daily.columns:
        df_daily["pressure"] = df_daily["pressure"] / 1000.0        
    if "precipitation" in df_daily.columns:
        df_daily["precipitation"] = df_daily["precipitation"] * 1000.0  
    if "radiation" in df_daily.columns:
        df_daily["radiation"] = df_daily["radiation"] / 1e6             

    df_daily["source"] = "era5"
    return df_daily

def _retrieve_month_piece(year: int, month: int, days: list, target_path: str):
    payload = {
        "product_type": "reanalysis",
        "variable": PARAMS,
        "year": f"{year:04d}",
        "month": f"{month:02d}",
        "day": [f"{d:02d}" for d in days],
        "time": [f"{h:02d}:00" for h in range(24)],
        "area": AREA,
        "format": "netcdf",
    }
    c.retrieve("reanalysis-era5-single-levels", payload, target_path)

def fetch_range(start_str: str, end_str: str) -> pd.DataFrame:
    start_dt = pd.to_datetime(start_str)
    end_dt = pd.to_datetime(end_str)
    all_frames = []

    rng = pd.date_range(start_dt, end_dt, freq="D")
    by_ym = {}
    for d in rng:
        by_ym.setdefault((d.year, d.month), []).append(d.day)

    tmpdir = tempfile.mkdtemp(prefix="era5_dl_")
    try:
        for (yy, mm), day_list in sorted(by_ym.items()):
            print(f"Fetching ERA5 {yy}-{mm:02d} ({len(day_list)} days)…")
            target_nc = os.path.join(tmpdir, f"era5_{yy}{mm:02d}.nc")
            try:
                _retrieve_month_piece(yy, mm, day_list, target_nc)
                size = os.path.getsize(target_nc) if os.path.exists(target_nc) else 0
                print(f"  → downloaded {size/1e6:.2f} MB to {target_nc}")
                if size < 10_000:
                    _log_head(target_nc)
                    print(f"   Skipping {yy}-{mm:02d}: file too small to be NetCDF.")
                    continue
                df_month = _process_downloaded_nc(target_nc)
                if not df_month.empty:
                    all_frames.append(df_month)
                else:
                    print(f"   No data rows parsed for {yy}-{mm:02d}.")
            except Exception as e:
                print(f"  {yy}-{mm:02d}: {e}")
                continue
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if not all_frames:
        return pd.DataFrame(columns=["date", "temperature", "dewpoint", "pressure",
                                     "wind_speed", "precipitation", "radiation", "source"])
    out = pd.concat(all_frames, ignore_index=True)
    out.drop_duplicates(subset=["date"], inplace=True)
    return out

# ============== MAIN ==============
def main():
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)

    today = pd.to_datetime(date.today())
    end_date = today - pd.Timedelta(days=5)   
    start_date = pd.to_datetime("2024-01-01")

    if os.path.exists(CSV_FILE):
        df_old = pd.read_csv(CSV_FILE, parse_dates=["date"])
        if not df_old.empty:
            df_old["date"] = pd.to_datetime(df_old["date"])
            last_date = df_old["date"].max()
            start_date = last_date + pd.Timedelta(days=1)
    else:
        df_old = pd.DataFrame()

    if start_date > end_date:
        print("Nothing new to fetch for ERA5.")
        return

    print(f"Fetching ERA5 data {start_date.date()} → {end_date.date()}")
    df_new = fetch_range(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    if df_new.empty:
        print(" No new ERA5 data parsed.")
        return

    df_new["date"] = pd.to_datetime(df_new["date"])

    if df_old.empty:
        df_all = df_new.copy()
    else:
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=["date"], keep="last", inplace=True)

    df_all = df_all.sort_values("date")
    df_all.to_csv(CSV_FILE, index=False)
    print(f"  Updated {CSV_FILE} with {len(df_new)} new rows.")

if __name__ == "__main__":
    main()
