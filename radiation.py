import asyncio
import os
import pandas as pd
from dateutil import relativedelta
import utils
from typing import List
from datetime import datetime

STATION_NAMES = {
    "47409": "abs", # Abashiri
    "47646": "tat", # Tateno
    "47807": "fua", # Fukoka
    "47412": "sap", # Sapporo
    "47918": "ish", # Ishigakijima
    "47991": "mnm", # Minamitorishima
}

class RadiationProcessor(utils.Processor):
    @staticmethod
    def extract_csv(file: str):
        with open(file, 'r', errors="ignore") as f:
            content = f.readlines()

            obs = []
            for line in content[18:]:
                if "--------------------------------------" in line:
                    break
                line = line[:-1]
                rad = line.split(" ")[-1]
                if 'X' in rad:
                    rad = 0
                rad = int(rad)
                obs.append(rad)

            df = pd.DataFrame({"rad": obs})
            df.to_csv(file, index=False)

    @staticmethod
    def merge_csvs(files: List[str], name: str):
        df = pd.DataFrame()
        for file in files:
            tmp_df = pd.read_csv(file)
            df = pd.concat([df, tmp_df], axis=0)
        
        df.to_csv(name + "_rad.csv", index=False)
        
    @staticmethod
    def format_csv(file: str, date: datetime):
        df = pd.read_csv(file)
        df["month"] = date.month
        df["year"] = date.year
        df["day"] = list(range(1, len(df)+1))
        
        df.to_csv(file, index=False)

def get_download_url_dl(name: str, year: int, month: int):
    return f"https://www.data.jma.go.jp/gmd/env/radiation/data/geppo/{year}{month:02}/DL{year}{month:02}_{name}.txt"

def get_download_url_df(name: str, year: int, month: int):
    return f"https://www.data.jma.go.jp/gmd/env/radiation/data/geppo/{year}{month:02}/DF{year}{month:02}_{name}.txt"

def get_download_url_dr(name: str, year: int, month: int):
    return f"https://www.data.jma.go.jp/gmd/env/radiation/data/geppo/{year}{month:02}/DR{year}{month:02}_{name}.txt"

def get_station(station: str, begin: datetime, end: datetime):
    download_dates = []
    while begin <= end:
        download_dates.append(begin)
        begin += relativedelta.relativedelta(months=1)
    
    urls = []

    loop = asyncio.get_event_loop()

    if station not in STATION_NAMES:
        return pd.DataFrame([], columns=["year", "month", "day", "dr", "dfr", "dlr"])
    name = STATION_NAMES[station]

    urls = [get_download_url_dl(name, date.year, date.month) for date in download_dates]
    loop.run_until_complete(utils.download_files(os.path.join(utils.CSV_DIR, f"{station[0]:05}" + "_DR"), urls, download_dates, RadiationProcessor))
    
    urls = [get_download_url_dl(name, date.year, date.month) for date in download_dates]
    loop.run_until_complete(utils.download_files(os.path.join(utils.CSV_DIR, f"{station[0]:05}" + "_DF"), urls, download_dates, RadiationProcessor))
    
    urls = [get_download_url_dl(name, date.year, date.month) for date in download_dates]
    loop.run_until_complete(utils.download_files(os.path.join(utils.CSV_DIR, f"{station[0]:05}" + "_DL"), urls, download_dates, RadiationProcessor))
        
    dr = pd.read_csv(os.path.join(utils.CSV_DIR, f"{station[0]:05}" + "_DR_rad.csv")).rename(columns={"rad": "dr"})
    df = pd.read_csv(os.path.join(utils.CSV_DIR, f"{station[0]:05}" + "_DF_rad.csv")).rename(columns={"rad": "dfr"})
    dl = pd.read_csv(os.path.join(utils.CSV_DIR, f"{station[0]:05}" + "_DL_rad.csv")).rename(columns={"rad": "dlr"})

    df = pd.merge(pd.merge(dr, df, on=["year", "month", "day"], how="outer"), dl, on=["year", "month", "day"], how="outer")
        
    return df
