import asyncio
import os
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import relativedelta
import utils
from typing import List, Tuple
from datetime import datetime
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)

def make_processor(mode: str):
    class WeatherProcessor(utils.Processor):
        @staticmethod
        def extract_csv(file: str):
            with open(file, 'r') as f:
                content = f.read()

                soup = BeautifulSoup(content, "html.parser")

                table_tags = soup.find_all("table")
                
                pos = 0
                if mode == "daily":
                    pos = 5
                if mode == "hourly" or mode == "10min":
                    pos = 4
                df = pd.read_html(str(table_tags[pos]))[0]
                df.columns = [" - ".join([col_part for i, col_part in enumerate(col)
                                        if col_part not in col[i+1:]]) for col in df.columns]
                df.to_csv(file, index=False)

        @staticmethod
        def merge_csvs(files: List[str], name: str):
            df = pd.DataFrame()
            for file in files:
                tmp_df = pd.read_csv(file)
                df = pd.concat([df, tmp_df], axis=0)

            df.to_csv(name+".csv", index=False)

        @staticmethod
        def format_csv(file: str, date: datetime):
            df = pd.read_csv(file)
            df["month"] = date.month
            df["year"] = date.year
            if mode == "hourly":
                df["day"] = date.day
                df.rename(columns={"時": "hour"}, inplace=True)
            elif mode == "10min":
                df["day"] = date.day
                df["hour"] = df.apply(lambda x: int(x["時分"].split(':')[0]), axis=1)
                df["minute"] = df.apply(lambda x: int(x["時分"].split(':')[1]), axis=1)
            elif mode == "daily":
                df.rename(columns={"日": "day"}, inplace=True)
            df = df.applymap(lambda x: x.replace(")", "") if isinstance(x, str) else x)
            df = df.applymap(lambda x: x.replace("]", "") if isinstance(x, str) else x)
            df.replace("--", np.nan, inplace=True)
            df.replace("-", np.nan, inplace=True)
            df.replace("///", np.nan, inplace=True)
            df.replace("×", np.nan, inplace=True)
            df.replace(" ", "", inplace=True)
            df.replace("", np.nan, inplace=True)

            df.to_csv(file, index=False)
            
    return WeatherProcessor


def get_download_url_master(station_id: str, station_code: str, year: int, month: int, day: int, mode: str) -> str:
    return f"https://www.data.jma.go.jp/obd/stats/etrn/view/{mode}_s1.php?prec_no={station_code}&block_no={station_id}&year={year}&month={month}&day={day}"

def get_download_url_norm(station_id: str, station_code: str, year: int, month: int, day: int, mode: str) -> str:
    return f"https://www.data.jma.go.jp/obd/stats/etrn/view/{mode}_a1.php?prec_no={station_code}&block_no={station_id:04}&year={year}&month={month}&day={day}"

def get_station(station: Tuple[int, str], begin: datetime, end: datetime, mode: str):
    download_dates = []
    delta = relativedelta.relativedelta(months=1)
    if mode == "daily":
        delta = relativedelta.relativedelta(months=1)
    elif mode == "hourly" or mode == "10min":
        delta = relativedelta.relativedelta(days=1)

    while begin <= end:
        download_dates.append(begin)
        begin += delta
    
    urls = []
    # master
    if str(station[0])[0] == "4":
        urls = [get_download_url_master(
            station[0], station[1], date.year, date.month, date.day, mode) for date in download_dates]
    else:
        urls = [get_download_url_norm(
            station[0], station[1], date.year, date.month, date.day, mode) for date in download_dates]

    loop = asyncio.get_event_loop()

    loop.run_until_complete(utils.download_files(
            os.path.join(utils.OUTPUT_DIR, f"{station[0]:05}"), urls, download_dates, make_processor(mode)))

    weather = pd.read_csv(os.path.join(utils.OUTPUT_DIR, f"{station[0]:05}") + ".csv").rename(columns={"日": "day"})
    
    return weather
