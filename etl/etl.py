import os
import glob
import re
import time
import csv

import pandas as pd
import timescaledb_model as tsdb
from timescaledb_model import initial_markets_data


TSDB = tsdb.TimescaleStockMarketModel
HOME = "/home/bourse/data/"
CLEAN_LAST_REGEX = re.compile(r"\(c\)\s*$")
BASE_SYMBOL_REGEX = re.compile(r"^1rP")
DATETIME_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2}(?:\.\d+)?))")


def timer_decorator(func):
    def wrapper(*args, **kwargs):
        t0 = time.time()
        res = func(*args, **kwargs)
        print(f"{func.__name__} → {time.time()-t0:.2f}s")
        return res
    return wrapper

@timer_decorator
def get_all_files(website: str, start, end) -> list[str]:
    if website == "euronext":
        pattern = os.path.join(HOME, website, "*")
    else:
        pattern = os.path.join(HOME, website, "20*", "*")

    files = glob.glob(pattern, recursive=True)
    df = pd.DataFrame({"filepath": files})
    df["filename"] = df["filepath"].str.split(os.sep).str[-1]
    df["date_str"] = df["filename"].str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    df["file_date"] = pd.to_datetime(df["date_str"], errors="coerce")
    mask = df["file_date"].between(start, end)
    return sorted(df.loc[mask, "filepath"].tolist())


def detect_header_csv(path: str) -> pd.DataFrame:
    with open(path, encoding="utf-8", errors="ignore") as f:
        lines = f.readlines() 
    for idx, line in enumerate(lines):
        if re.search(r"\bName\b.*\bISIN\b.*\bSymbol\b", line):
            cols = re.split(r"\s{2,}|\t", line.strip())
            break
    else:
        raise ValueError(f"Header introuvable dans {path}")
    return pd.read_csv(
        path,
        engine="python",
        sep=r"\s{2,}|\t",
        header=None,
        names=cols,
        skiprows=idx + 1,
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip"
    )


def detect_header_xlsx(path: str) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None, engine="openpyxl")
    for idx, row in raw.iterrows():
        vals = row.astype(str).str.strip().tolist()
        if {"Name", "ISIN", "Symbol"}.issubset(vals):
            header = vals
            df = raw.iloc[idx + 1 :].copy()
            df.columns = header
            df = df[df["ISIN"].notna() & df["Symbol"].notna()]
            return df
    raise ValueError(f"Header introuvable dans {path}")


def compute_csv(path: str, start_dt, end_dt) -> pd.DataFrame:
    df = detect_header_csv(path)
    df = df.dropna(subset=["Last Date/Time", "Symbol"])
    df["datetime"] = pd.to_datetime(
        df["Last Date/Time"].str.strip(),
        format="%d/%m/%y %H:%M",
        errors="coerce"
    )
    df = df[(df["datetime"] >= start_dt) & (df["datetime"] <= end_dt)]
    return pd.DataFrame({
        "date":   df["datetime"].dt.floor("D"),
        "symbol": df["Symbol"],
        "cid":    None,
        "open":   pd.to_numeric(df["Open"],  errors="coerce"),
        "close":  pd.to_numeric(df["Last"],  errors="coerce"),
        "high":   pd.to_numeric(df["High"],  errors="coerce"),
        "low":    pd.to_numeric(df["Low"],   errors="coerce"),
        "volume": pd.to_numeric(df["Volume"],errors="coerce"),
        "mean":   None,
        "std":    None,
    }).dropna(subset=["date","open","close","high","low","volume"])


def compute_xlsx(path: str, start_dt, end_dt) -> pd.DataFrame:
    df = detect_header_xlsx(path)
    df = df.dropna(subset=["last Trade MIC Time", "Symbol"])
    df["datetime"] = pd.to_datetime(
        df["last Trade MIC Time"].str.strip(),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    df = df[(df["datetime"] >= start_dt) & (df["datetime"] <= end_dt)]
    return pd.DataFrame({
        "date":   df["datetime"].dt.floor("D"),
        "symbol": df["Symbol"],
        "cid":    None,
        "open":   pd.to_numeric(df["Open Price"], errors="coerce"),
        "close":  pd.to_numeric(df["last Price"],  errors="coerce"),
        "high":   pd.to_numeric(df["High Price"], errors="coerce"),
        "low":    pd.to_numeric(df["low Price"],  errors="coerce"),
        "volume": pd.to_numeric(df["Volume"],     errors="coerce"),
        "mean":   None,
        "std":    None,
    }).dropna(subset=["date","open","close","high","low","volume"])


def compute_gz2(path: str) -> pd.DataFrame:
    return pd.read_pickle(path)

@timer_decorator
def fill_missing_daystocks(start, end, db: TSDB):
    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)
    df_existing = db.df_query(
        "SELECT date, cid FROM daystocks WHERE date >= '%s' AND date <= '%s'" % (start_dt, end_dt)
    )
    df_existing['date'] = pd.to_datetime(df_existing['date']).dt.floor('D')
    comps = db.df_query("SELECT id AS cid FROM companies")
    cids = comps['cid'].unique()
    full_dates = pd.date_range(start_dt.floor('D'),
		end_dt.floor('D'),
		freq='B',
		tz='UTC')
    full = pd.MultiIndex.from_product([full_dates, cids], names=['date','cid']).to_frame(index=False)
    merged = full.merge(df_existing.drop_duplicates(), on=['date','cid'], how='left', indicator=True)
    missing = merged[merged['_merge']=='left_only'][['date','cid']]
    if missing.empty:
        print("Aucun jour manquant à remplir.")
        return
    df_stocks = db.df_query(
        "SELECT date, cid, value, volume FROM stocks WHERE date >= '%s' AND date <= '%s'" % (start_dt, end_dt)
    )
    df_stocks['date'] = pd.to_datetime(df_stocks['date']).dt.floor('D')
    agg = df_stocks.groupby(['date','cid']).agg(
        open=('value','first'),
        close=('value','last'),
        high=('value','max'),
        low=('value','min'),
        volume=('volume','sum'),
        mean=('value','mean'),
        std=('value','std')
    ).reset_index()
    to_insert = missing.merge(agg, on=['date','cid'], how='inner')
    if to_insert.empty:
        print("Pas de données Boursorama pour les jours manquants.")
        return
    db.df_write(to_insert, 'daystocks', if_exists='append', index=False)
    db.commit()
    #print(f"✓ {len(to_insert)} jours manquants remplis depuis Boursorama.")

@timer_decorator
def store_files_done(files: list[str], db: TSDB) -> None:
    if not files:
        return
    placeholders = ",".join("(%s)" for _ in files)
    sql = (
        f"INSERT INTO file_done (name) VALUES {placeholders} "
        "ON CONFLICT (name) DO NOTHING;"
    )
    db.execute(sql, tuple(files), commit=True)
    #print(f"✓ {len(files)} fichier insérés dans file_done.")

@timer_decorator
def store_markets(db: TSDB):
    """
    Truncate and reload the 'markets' table using initial_markets_data
    from timescaledb_model.py.
    """
    cols = ["id", "name", "alias", "boursorama", "sws", "euronext"]
    df_markets = pd.DataFrame(initial_markets_data, columns=cols)

    db.execute("TRUNCATE TABLE markets RESTART IDENTITY CASCADE;", commit=True)
    db.df_write(df_markets, "markets", if_exists="append", index=False)
    db.commit()
    #print(f"✓ {len(df_markets)} markets inserted into 'markets'.")

@timer_decorator
def store_companies(files: list[str], db: TSDB):
    market_map = {
        "Euronext Paris":            6,
        "Euronext Growth Paris":     6,
        "Euronext Access Paris":     6,
        "Euronext Brussels, Paris":  6,
    }

    comps = []
    for file in files:
        if file.endswith(".csv"):
            df = detect_header_csv(file)
        else:
            df = detect_header_xlsx(file)
        df = df[df["ISIN"].notna() & df["Symbol"].notna()]
        comps.append(df[["Name","ISIN","Symbol","Market"]])

    allc = pd.concat(comps, ignore_index=True)
    allc = allc.drop_duplicates(subset=["Symbol","ISIN"])
    allc["mid"] = allc["Market"].map(market_map).fillna(0).astype(int)

    df_comp = pd.DataFrame({
        "name"      : allc["Name"],
        "mid"       : allc["mid"],
        "symbol"    : allc["Symbol"],
        "isin"      : allc["ISIN"],
        "boursorama": None,
        "euronext"  : allc["Symbol"],
        "pea"       : False,
        "sector1"   : None,
        "sector2"   : None,
        "sector3"   : None,
    })
    db.execute("TRUNCATE TABLE companies CASCADE;", commit=True)
    db.execute("TRUNCATE TABLE daystocks, stocks RESTART IDENTITY CASCADE;", commit=True)
    db.execute("ALTER SEQUENCE company_id_seq RESTART WITH 1;", commit=True)
    db.df_write(df_comp, "companies", if_exists="append", index=False)
    db.commit()
    #print(f"✓ {len(df_comp)} entreprises insérées dans companies.")



def process_stocks(df, file_path, symbol_to_cid):
    filename = os.path.basename(file_path)
    m = DATETIME_REGEX.search(filename)
    if not m:
        raise ValueError(f"Invalid filename: {filename}")

    file_dt = pd.to_datetime(m.group(1), errors="coerce")
    if pd.isna(file_dt):
        raise ValueError(f"Invalid datetime: {filename}")

    df = df[['symbol', 'last', 'volume']].dropna()
    df['value'] = pd.to_numeric(df['last'].str.replace(CLEAN_LAST_REGEX, '', regex=True).str.strip(), errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    valid = df['value'].notna() & df['volume'].notna()
    df = df.loc[valid]

    symbols = df['symbol'].str.replace(BASE_SYMBOL_REGEX, '', regex=True)
    cids = symbols.map(symbol_to_cid)

    valid_cids = cids.notna()
    df = df.loc[valid_cids]
    df = df.assign(
        cid=cids.loc[valid_cids].astype(int),
        date=file_dt
    )
    return df[['date', 'cid', 'value', 'volume']]




@timer_decorator
def store_files(start: str, end: str, website: str, db: TSDB):
    start_dt, end_dt = pd.to_datetime(start), pd.to_datetime(end)
    files = get_all_files(website, start_dt, end_dt)
    store_files_done(files, db)

    if website == 'euronext':
        store_companies(files, db)
        map_df = db.df_query("SELECT id AS cid, euronext AS symbol FROM companies")
        symbol_to_cid = dict(zip(map_df['symbol'], map_df['cid']))
        all_days = []
        for f in files:
            df_day = compute_csv(f, start_dt, end_dt) if f.endswith('.csv') else compute_xlsx(f, start_dt, end_dt)
            df_day = df_day.loc[df_day['symbol'].isin(symbol_to_cid)]
            df_day['cid'] = df_day['symbol'].map(symbol_to_cid).astype(int)
            all_days.append(df_day.drop(columns=['symbol']))
        if all_days:
            full = pd.concat(all_days, ignore_index=True)
            db.df_write(full, 'daystocks', if_exists='append', index=False)
            db.commit()

    else:
        db.execute(
            "DELETE FROM stocks WHERE date >= %s AND date <= %s;",
            (start_dt, end_dt), commit=True
        )
        comp = db.df_query("SELECT id AS cid, symbol FROM companies")
        symbol_to_cid = dict(zip(comp['symbol'], comp['cid']))
        stocks_list = []
        for f in files:
            try:
                df_raw = compute_gz2(f)
                stocks_list.append(process_stocks(df_raw, f, symbol_to_cid))
            except ValueError:
                continue
        if stocks_list:
            full = pd.concat(stocks_list, ignore_index=True)
            db.df_write(full, 'stocks', if_exists='append', index=False)
            db.commit()




if __name__ == "__main__":
    print("Go Extract Transform and Load")
    pd.set_option("display.max_columns", None)
    db = TSDB("bourse", "ricou", "db", "monmdp", remove_all=True)
    start_date = "2020-08-15"
    end_date = "2020-08-20"
    store_markets(db)
    db.execute("TRUNCATE TABLE file_done;", commit=True)
    store_files(start_date, end_date, "euronext", db)
    store_files(start_date, end_date, "bourso", db)
    fill_missing_daystocks(start_date, end_date, db)
    print("Done ETL.")


