import pandas as pd, os
from datetime import date
import sys

URL = ("https://api.ipma.pt/open-data/observation/climate/temperature-mean/ "
       "setubal/palmela.csv").replace(" ", "")
OUTFILE = "/Users/chandadiwakar/Downloads/Smart Produce/data/ipma.csv"

def main():
    df_new = pd.read_csv(URL)
    df_new.rename(columns={'dataHora': 'datetime'}, inplace=True)
    df_new['datetime'] = pd.to_datetime(df_new['datetime'])
    os.makedirs(os.path.dirname(OUTFILE), exist_ok=True)
    if os.path.exists(OUTFILE):
        old = pd.read_csv(OUTFILE, parse_dates=['datetime'])
        df = pd.concat([old, df_new]).drop_duplicates('datetime').sort_values('datetime')
    else:
        df = df_new
    df.to_csv(OUTFILE, index=False)
    print(f"[IPMA] Saved {len(df)} rows.")

if __name__ == '__main__':
    main()

