#!/usr/bin/env python3
import os, re, time, glob, bz2
import numpy as np, pandas as pd
import timescaledb_model as tsdb

TSDB  = tsdb.TimescaleStockMarketModel
HOME  = "/home/bourse/data/"
TRIAL_SEPS = [',',';','\t']
_TS_RE = re.compile(
    r'(\d{4}-\d{2}-\d{2}[ _T]\d{2}[:\-]\d{2}[:\-]\d{2}(?:\.\d+)?)'
)

def timer_decorator(fn):
    def wrapper(*a, **kw):
        t0 = time.time()
        res = fn(*a, **kw)
        print(f"{fn.__name__} completed in {time.time()-t0:.2f}s")
        return res
    return wrapper

def detect_and_load_csv(path, compression):
    """robust CSV/TSV loader with delimiter sniffing & compression handling"""
    try:
        return pd.read_csv(path, sep=None, engine='python',
                           compression=compression, encoding='latin-1')
    except Exception:
        open_func = {'bz2': bz2.open}.get(compression, open)
        with open_func(path, 'rt', encoding='latin-1', errors='ignore') as fh:
            header = fh.readline()
            for sep in TRIAL_SEPS:
                if sep in header:
                    fh.seek(0)
                    return pd.read_csv(fh, sep=sep, encoding='latin-1')
        return pd.read_csv(path, sep=',', engine='python',
                           compression=compression, encoding='latin-1')

def timestamp_from_filename(path: str) -> pd.Timestamp|None:
    """pull  ‘YYYY-MM-DD hh:mm:ss[.ffffff]’  from *anywhere* in basename"""
    base = os.path.basename(path)
    while True:
        root, ext = os.path.splitext(base)
        if not ext:
            break
        base = root
    m = _TS_RE.search(base)
    if not m:
        return None
    try:
        return pd.to_datetime(m.group(1))
    except ValueError:
        return None

@timer_decorator
def store_files(start: str, end: str, source: str, db: TSDB) -> None:
    start_dt, end_dt = map(pd.to_datetime, (start, end))

    if source not in {'euronext', 'bourso'}:
        print(f"Unknown source {source}")
        return

    src_dir = os.path.join(HOME, source)
    files   = [f for f in glob.glob(os.path.join(src_dir, '**', '*'),
                                    recursive=True) if os.path.isfile(f)]
    if not files:
        print(f"No data files in {src_dir}")
        return

    frames = []
    for fn in files:
        ext = fn.lower()

        try:
            if source == 'euronext':
                if ext.endswith(('.csv','.txt','.tsv','.bz2','.gz','.zip','.gz2')):
                    df = detect_and_load_csv(fn, compression='infer')
                elif ext.endswith(('.xls', '.xlsx')):
                    df = pd.read_excel(fn)
                else:
                    continue
            else:                       # bourso
                if ext.endswith(('.pkl','.bz2','.gz','.gz2','.zip')):
                    try:
                        df = pd.read_pickle(fn, compression='infer')
                    except Exception:
                        comp = 'gzip' if ext.endswith('.gz2') else 'infer'
                        df  = detect_and_load_csv(fn, compression=comp)
                elif ext.endswith(('.csv','.txt','.tsv')):
                    df = detect_and_load_csv(fn, compression=None)
                else:
                    continue
        except Exception as e:
            print(f"Failed loading {fn}: {e}")
            continue

        if df is None or df.empty:
            continue

        df.columns = [re.sub(r'[^0-9A-Za-z]+', '_', str(c)).lower().strip('_')
                      for c in df.columns]

        if source == 'bourso':
            print(f"Loaded bourso file {fn} columns: {df.columns.tolist()}")

        price_map = dict(open='price_open', high='price_high',
                         low='price_low', close='price_close', last='price_close')
        df.rename(columns={k:v for k,v in price_map.items() if k in df.columns},
                  inplace=True)

        if source == 'euronext':
            for alt in ('timestamp','last_date_time','last_datetime',
                        'last_trade_mic_time','closing_price_datetime'):
                if alt in df.columns:
                    df.rename(columns={alt: 'timestamp'}, inplace=True)
                    break

            if 'timestamp' not in df.columns and {'date','time'}.issubset(df.columns):
                df['timestamp'] = (df['date'].astype(str).str.strip() + ' ' +
                                   df['time'].astype(str).str.strip())

            ts_dupes = [c for c in df.columns if c != 'timestamp' and
                                               c.startswith('timestamp')]
            if ts_dupes:
                df.drop(columns=ts_dupes, inplace=True)

        else:
            if 'symbol' not in df.columns:
                for alt in ('ticker','isin','name'):
                    if alt in df.columns:
                        df.rename(columns={alt:'symbol'}, inplace=True)
                        break
            if 'timestamp' not in df.columns:
                for alt in ('date','datetime','date_time','last_datetime'):
                    if alt in df.columns:
                        df.rename(columns={alt:'timestamp'}, inplace=True)
                        break
            if 'timestamp' not in df.columns and isinstance(df.index, pd.DatetimeIndex):
                df.index.name = None
                df = df.reset_index().rename(columns={'index':'timestamp'})
            if 'timestamp' not in df.columns:
                ts = timestamp_from_filename(fn)
                if ts is not None:
                    df['timestamp'] = ts
                    print(f"[bourso] {os.path.basename(fn)} ⇒ timestamp {ts}")

        if 'timestamp' not in df.columns or 'symbol' not in df.columns:
            print(f"Missing critical columns in {fn}, columns were: {df.columns.tolist()}")
            continue

        if source == 'euronext':
            df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True, errors='coerce')
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'],
                                             format='%Y-%m-%d %H:%M:%S.%f',
                                             errors='coerce')
            mask = df['timestamp'].isna()
            if mask.any():
                df.loc[mask,'timestamp'] = pd.to_datetime(df.loc[mask,'timestamp'],
                                                          format='%Y-%m-%d %H:%M:%S',
                                                          errors='coerce')
            mask = df['timestamp'].isna()
            if mask.any():
                df.loc[mask,'timestamp'] = pd.to_datetime(df.loc[mask,'timestamp'],
                                                          format='%d/%m/%Y %H:%M:%S',
                                                          dayfirst=True,
                                                          errors='coerce')

        df.dropna(subset=['timestamp'], inplace=True)

        df = df[(start_dt <= df['timestamp']) & (df['timestamp'] < end_dt)]
        if df.empty:
            continue

        df['symbol'] = df['symbol'].astype(str).str.strip()
        for col in ('price_open','price_high','price_low','price_close','volume'):
            if col in df.columns:
                df[col] = (df[col].astype(str)
                                 .replace({'-':np.nan,'':np.nan}, regex=False)
                                 .str.replace(',','', regex=False)
                                 .pipe(pd.to_numeric, errors='coerce'))
        if 'volume' not in df.columns:
            df['volume'] = np.nan

        frames.append(df)

    if not frames:
        print(f"No valid data loaded for {source}")
        return

    data = pd.concat(frames, ignore_index=True).drop_duplicates(['symbol','timestamp'])

    for sym in data['symbol'].unique():
        if not db.raw_query("SELECT id FROM companies WHERE symbol=%s", (sym,)):
            db.execute("INSERT INTO companies(name,symbol,euronext,boursorama)"
                       "VALUES(%s,%s,%s,%s)",
                       (sym, sym,
                        sym if source=='euronext' else None,
                        sym if source=='bourso'   else None))
    db.commit()

    cid = {sym: cid for cid, sym in db.raw_query("SELECT id,symbol FROM companies")}
    data['cid'] = data['symbol'].map(cid)

    if source == 'euronext':
        data['mean'] = data[['price_open','price_high','price_low','price_close']].mean(1)
        data['std']  = data[['price_open','price_high','price_low','price_close']].std(1)
        out = data.rename(columns={'timestamp':'date',
                                   'price_open':'open',
                                   'price_close':'close',
                                   'price_high':'high',
                                   'price_low':'low'})
        db.df_write(out[['date','cid','open','close','high','low',
                         'volume','mean','std']], 'daystocks', commit=True)
    else:
        out = data.rename(columns={'timestamp':'date', 'price_close':'value'})
        db.df_write(out[['date','cid','value','volume']], 'stocks', commit=True)

if __name__ == '__main__':
    print("Starting ETL process...")
    db = TSDB('bourse','ricou','db','monmdp')
    store_files('2019-01-01','2024-12-31','euronext', db)
    store_files('2019-01-01','2024-12-31','bourso',   db)
