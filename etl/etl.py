import os
import glob
import time
import re
import csv

import pandas as pd
import timescaledb_model as tsdb

TSDB = tsdb.TimescaleStockMarketModel
HOME = "/home/bourse/data/"   # on s'attend à sous-dossiers euronext/ et boursorama/

def get_all_files(website: str, start, end) -> list[str]:
    result = []
    if website == "euronext":
        pattern = os.path.join(HOME, website, "*")
    else:
        pattern = os.path.join(HOME, website, "20*", "*")
    files = glob.glob(pattern, recursive=True)
    for file in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(file))
        file_date = pd.to_datetime(m.group(1), errors='coerce')
        if start <= file_date <= end:
            result.append(file)
    return sorted(result)


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

# ----------------------------------------
# Décorateur de timing
# ----------------------------------------
def timer_decorator(func):
    def wrapper(*args, **kwargs):
        t0 = time.time()
        res = func(*args, **kwargs)
        print(f"{func.__name__} → {time.time()-t0:.2f}s")
        return res
    return wrapper

def fill_missing_daystocks(start, end, db: TSDB):
    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)
    # Charger déjà inséré daystocks
    df_existing = db.df_query(
        "SELECT date, cid FROM daystocks WHERE date >= '%s' AND date <= '%s'" % (start_dt, end_dt)
    )
    df_existing['date'] = pd.to_datetime(df_existing['date']).dt.floor('D')

    # Obtenir tous les cid connus
    comps = db.df_query("SELECT id AS cid FROM companies")
    cids = comps['cid'].unique()

    # Générer toutes combinaisons de date x cid
    full_dates = pd.date_range(start_dt.floor('D'),
		end_dt.floor('D'),
		freq='B',
		tz='UTC')
    full = pd.MultiIndex.from_product([full_dates, cids], names=['date','cid']).to_frame(index=False)

    # Identifier les manquants
    merged = full.merge(df_existing.drop_duplicates(), on=['date','cid'], how='left', indicator=True)
    missing = merged[merged['_merge']=='left_only'][['date','cid']]
    if missing.empty:
        print("Aucun jour manquant à remplir.")
        return

    # Aggreger depuis stocks (10min) pour combler
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
    print(f"✓ {len(to_insert)} jours manquants remplis depuis Boursorama.")

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
    print(f"✓ {len(df_comp)} entreprises insérées dans companies.")


def store_stocks(df, file, start_dt, end_dt, db: TSDB) -> pd.DataFrame:
    m = re.search(
        r"(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2}(?:\.\d+)?))",
        os.path.basename(file)
    )
    file_dt = pd.to_datetime(m.group(1), errors="coerce")
    df = df.copy()
    df["last"] = df["last"].str.replace(r"\(c\)\s*$", "", regex=True).str.strip()
    df["value"]  = pd.to_numeric(df["last"],   errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["symbol","value","volume"])
    df["base_symbol"] = df["symbol"].str.replace(r"^1rP", "", regex=True)
    comp = db.df_query("SELECT id AS cid, symbol FROM companies")
    symbol2cid = dict(zip(comp["symbol"], comp["cid"]))
    df["cid"] = df["base_symbol"].map(symbol2cid)
    df = df.dropna(subset=["cid"])
    df["cid"] = df["cid"].astype(int)
    df["date"] = file_dt
    df_to_write = df[["date","cid","value","volume"]]
    db.df_write(df_to_write, "stocks", if_exists="append", index=False)
    db.commit()

@timer_decorator
def store_files(start: str, end: str, website: str, db: TSDB):
    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)
    files = get_all_files(website, start_dt, end_dt)
    if website == "euronext":
        store_companies(files, db)
        map_df = db.df_query("SELECT id, euronext AS symbol FROM companies")
        symbol_to_cid = dict(zip(map_df["symbol"], map_df["id"]))
        all_days = []
        for file in files:
            if file.endswith(".csv"):
                df_day = compute_csv(file, start_dt, end_dt)
            else:
                df_day = compute_xlsx(file, start_dt, end_dt)
            df_day["cid"] = df_day["symbol"].map(symbol_to_cid)
            df_day = df_day.dropna(subset=["cid"])
            df_day["cid"] = df_day["cid"].astype(int)
            all_days.append(df_day)
        full_df = pd.concat(all_days, ignore_index=True).drop(columns=["symbol"])
        db.df_write(full_df, "daystocks", if_exists="append", index=False)
        print(f"Insertion en base : {len(full_df):,} lignes")

    else:
        db.execute("DELETE FROM stocks WHERE date >= %s AND date <= %s;",(start_dt, end_dt), commit=True)
        for file in files:
            df_day = compute_gz2(file)
            store_stocks(df_day, file, start_dt, end_dt, db)
    # db.commit()


# ----------------------------------------
# Main
# ----------------------------------------
if __name__ == "__main__":
    print("Go Extract Transform and Load")
    pd.set_option("display.max_columns", None)
    db = TSDB("bourse", "ricou", "db", "monmdp")
    start_date = "2020-06-15"
    end_date = "2020-06-20"
    store_files(start_date, end_date, "euronext", db)
    store_files(start_date, end_date, "bourso", db)
    fill_missing_daystocks(start_date, end_date, db)
    print("Done ETL.")
