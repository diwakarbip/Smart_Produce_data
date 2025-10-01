#This obtains/updates the csv file witht the data from the openmeteo source

import requests
import pandas as pd
import os
import sys
from datetime import datetime, timedelta, date

LAT, LON = 38.5692, -8.9014
OUTFILE = '/Users/chandadiwakar/Desktop/SmartProduce/data/open_meteo.csv'
CHUNK_DAYS = 30
OPENMETEO_API = 'https://archive-api.open-meteo.com/v1/archive'
NASA_POWER_DAILY = 'https://power.larc.nasa.gov/api/temporal/daily/point'

# Parameters to request from Open-Meteo (only what exists)
OPENMETEO_PARAMS = [
    'temperature_2m',
    'relative_humidity_2m',
    'wind_direction_10m',
    'wind_speed_10m',
    'precipitation',
    'shortwave_radiation',
    'pressure_msl',
    'apparent_temperature',
    'soil_moisture_0_to_7cm',
]

def fetch_openmeteo_chunk(start_date, end_date):
    params = {
        'latitude': LAT,
        'longitude': LON,
        'start_date': start_date,
        'end_date': end_date,
        'hourly': ','.join(OPENMETEO_PARAMS),
        'timezone': 'auto',
    }
    print(f"[Open-Meteo] Requesting {start_date} to {end_date}")
    r = requests.get(OPENMETEO_API, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if 'hourly' not in data or 'time' not in data['hourly']:
        print(f"[Open-Meteo] No hourly data for {start_date} to {end_date}")
        return pd.DataFrame()

    df = pd.DataFrame({'datetime': pd.to_datetime(data['hourly']['time'])})
    for param in OPENMETEO_PARAMS:
        df[param] = data['hourly'].get(param, [None]*len(df))

    return df

def fetch_nasa_uv_daily(start_date, end_date):

    params = {
        'start': start_date.replace('-', ''),
        'end': end_date.replace('-', ''),
        'latitude': LAT,
        'longitude': LON,
        'parameters': 'ALLSKY_SFC_UV_INDEX',
        'community': 'AG',
        'format': 'JSON'
    }
    print(f"[NASA POWER] Requesting UV {start_date} to {end_date}")
    r = requests.get(NASA_POWER_DAILY, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    try:
        uv_data = data['properties']['parameter']['ALLSKY_SFC_UV_INDEX']
    
        rows = []
        for k, v in uv_data.items():
            dt = datetime.strptime(k, '%Y%m%d').date()
            rows.append({'date': dt, 'uv_index': v})
        return pd.DataFrame(rows)
    except Exception:
        print("[NASA POWER] UV data missing or invalid")
        return pd.DataFrame()

def add_uv_to_df(df, start_date, end_date):
   
    if 'uv_index' in df.columns and df['uv_index'].notna().any():
        return df  

    uv_df = fetch_nasa_uv_daily(start_date, end_date)
    if uv_df.empty:
        df['uv_index'] = None
        return df

    df['date'] = df['datetime'].dt.date
    uv_map = uv_df.set_index('date')['uv_index'].to_dict()
    df['uv_index'] = df['date'].map(uv_map)
    df.drop(columns=['date'], inplace=True)
    return df

def chunk_dates(start_date, end_date, chunk_days=CHUNK_DAYS):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days-1), end)
        yield current.isoformat(), chunk_end.isoformat()
        current = chunk_end + timedelta(days=1)

def save_incremental(df):
    os.makedirs(os.path.dirname(OUTFILE), exist_ok=True)
    if os.path.exists(OUTFILE):
        old = pd.read_csv(OUTFILE, parse_dates=['datetime'])
        combined = pd.concat([old, df], ignore_index=True)
        combined.drop_duplicates(subset=['datetime'], inplace=True)
        combined.sort_values('datetime', inplace=True)
        combined.to_csv(OUTFILE, index=False)
        print(f"[Open-Meteo] Appended data, total rows: {len(combined)}")
    else:
        df.to_csv(OUTFILE, index=False)
        print(f"[Open-Meteo] Saved new file with {len(df)} rows")

def main(start_date=None, end_date=None):
    if not start_date:
        start_date = '2024-01-01'
    if not end_date:
        end_date = (date.today() - timedelta(days=1)).isoformat()

    print(f"[Open-Meteo] Updating from {start_date} to {end_date}")

    all_chunks = []
    for chunk_start, chunk_end in chunk_dates(start_date, end_date):
        chunk_df = fetch_openmeteo_chunk(chunk_start, chunk_end)
        if not chunk_df.empty:
            all_chunks.append(chunk_df)

    if not all_chunks:
        print("[Open-Meteo] No data fetched.")
        return

    df = pd.concat(all_chunks, ignore_index=True)
    df = add_uv_to_df(df, start_date, end_date)
    df.sort_values('datetime', inplace=True)

    save_incremental(df)

if __name__ == '__main__':
    args = sys.argv[1:]
    main(*args)
