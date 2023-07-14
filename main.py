import asyncio
import os
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import relativedelta
import sys
import radiation as rad
import utils
from typing import List
from datetime import datetime
import logging

class WeatherProcessor(utils.Processor):
    @staticmethod
    def extract_csv(file: str):
        with open(file, 'r') as f:
            content = f.read()

            soup = BeautifulSoup(content, "html.parser")

            table_tags = soup.find_all("table")
            df = pd.read_html(str(table_tags[5]))[0]
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
        df.replace("--", 0, inplace=True)
        df.replace("///", 0, inplace=True)

        df.to_csv(file, index=False)


def get_download_url(station_id: str, station_code: str, year: int, month: int, day: int) -> str:
    return f"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={station_code}&block_no={station_id}&year={year}&month={month}&day={day}"


if __name__ == "__main__":
    begin_day_string = sys.argv[1]
    end_day_string = sys.argv[2]
    
    begin_day = datetime.strptime(begin_day_string, "%Y/%m/%d")
    end_day = datetime.strptime(end_day_string, "%Y/%m/%d")

    download_dates = []
    curr_day = begin_day.replace(day=1)

    while curr_day <= end_day:
        download_dates.append(curr_day)
        curr_day += relativedelta.relativedelta(months=1)

    stations_df = pd.read_csv(utils.STATIONS)
    station_ids = stations_df["id"].to_numpy()
    station_codes = [str(code)[:2] for code in stations_df["code"].to_numpy()]

    stations = list(zip(station_ids, station_codes))

    urls = []

    loop = asyncio.get_event_loop()

    for station in stations:
        urls = [get_download_url(
            station[0], station[1], date.year, date.month, date.day) for date in download_dates]
        loop.run_until_complete(utils.download_files(
            os.path.join(utils.OUTPUT_DIR, f"{station[0]:05}"), urls, download_dates, WeatherProcessor))
        
        radiation = rad.get_station(f"{station[0]:05}", begin_day, end_day)
        logging.info(f"Downloaded station {station[0]}")

        weather = pd.read_csv(os.path.join(utils.OUTPUT_DIR, f"{station[0]:05}") + ".csv").rename(columns={"日": "day"})
        
        for c in weather.columns:
            try:
                if c in ["month", "year", "day"]:
                    continue
                weather[c] = weather[c].astype("str").str.replace(")", "")
                weather[c] = weather[c].astype("str").str.replace("×", "")
                weather[c] = weather[c].astype("str").str.replace("--", "")
                weather[c] = weather[c].astype("str").str.replace("]", "")
                weather[c] = weather[c].astype("str").str.replace(" ", "")
                weather[c] = weather[c].replace('', 0)
            except Exception as e:
                logging.exception(station, c, e)

        weather = pd.merge(weather, radiation, on=["year", "month", "day"], how="outer")
        weather.to_csv(os.path.join(utils.OUTPUT_DIR, f"{station[0]:05}") + ".csv", index=False)
        
    logging.info("Done!")
