import aiohttp
import asyncio
import os
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from dateutil import relativedelta

CSV_DIR = "csv"
OUTPUT_DIR = "output"
Path(CSV_DIR).mkdir(parents=True, exist_ok=True)
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

async def download_file(session, url, name):
    downloaded = False
    while not downloaded:
        try:
            async with session.get(url, timeout=10000) as response:
                with open(name, "wb") as f:
                    while True:
                        chunk = await response.content.read(100*1024*1024)
                        if not chunk:
                            break
                        f.write(chunk)
                downloaded = True
        except RuntimeError as e:
            print(e)
            pass


def extract_csv(file):
    with open(file, 'r') as f:
        content = f.read()

        soup = BeautifulSoup(content, 'html.parser')

        table_tags = soup.find_all('table')
        df = pd.read_html(str(table_tags[5]))[0]
        df.columns = [' - '.join([col_part for i, col_part in enumerate(col)
                                 if col_part not in col[i+1:]]) for col in df.columns]
        df.to_csv(file, index=False)


def merge_csvs(files, name):
    df = pd.DataFrame()
    for file in files:
        tmp_df = pd.read_csv(file)
        df = pd.concat([df, tmp_df], axis=0)

    df.to_csv(name+".csv", index=False)


def format_csv(file, date):
    df = pd.read_csv(file)
    df["month"] = date.month
    df["year"] = date.year
    df.replace("--", 0, inplace=True)
    df.replace("///", 0, inplace=True)

    df.to_csv(file, index=False)
    

async def download_files(urls, name, dates):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5), timeout=10000) as session:
        tasks = []
        filenames = [os.path.join(CSV_DIR, f"{l}.csv")
                     for l in range(len(urls))]
        for i in range(len(urls)):
            task = asyncio.create_task(
                download_file(session, urls[i], filenames[i]))
            tasks.append(task)
        await asyncio.gather(*tasks)
        await asyncio.sleep(1)

        for file in filenames:
            extract_csv(file)
        for i, file in enumerate(filenames):
            format_csv(file, dates[i])
        merge_csvs(filenames, name)


def get_download_url(station_id, station_code, year, month, day) -> str:
    return f"https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={station_code}&block_no={station_id}&year={year}&month={month}&day={day}"


if __name__ == "__main__":
    begin_day_string = "2010/01/01"
    end_day_string = "2011/12/31"

    print("Starting...")
    begin_day = datetime.strptime(begin_day_string, "%Y/%m/%d")
    end_day = datetime.strptime(end_day_string, "%Y/%m/%d")

    download_dates = []
    curr_day = begin_day.replace(day=1)

    while curr_day <= end_day:
        download_dates.append(curr_day)
        curr_day += relativedelta.relativedelta(months=1)

    stations_df = pd.read_csv("stations.csv")
    station_ids = stations_df["id"].to_numpy()
    station_codes = [str(code)[:2] for code in stations_df["code"].to_numpy()]

    stations = list(zip(station_ids, station_codes))

    urls = [get_download_url]

    loop = asyncio.get_event_loop()

    for station in stations:
        print(f"Downloading station {station[0]}...")
        urls = [get_download_url(
            station[0], station[1], date.year, date.month, date.day) for date in download_dates]
        loop.run_until_complete(download_files(
            urls, os.path.join(OUTPUT_DIR, f"{station[0]:05}"), download_dates))
        print(f"Downloaded station {station[0]}")
        

    print("Formatting...")
    for station in stations:
        weather = pd.read_csv(os.path.join(OUTPUT_DIR, f"{station[0]:05}") + ".csv")
        
        for c in weather.columns:
            try:
                if c in ["month", "year", "day"]:
                    continue
                weather[c] = weather[c].astype("str").str.replace(')', '')
                weather[c] = weather[c].astype("str").str.replace('×', '')
                weather[c] = weather[c].astype("str").str.replace('--', '')
                weather[c] = weather[c].astype("str").str.replace(']', '')
                weather[c] = weather[c].astype("str").str.replace(' ', '')
                weather[c] = weather[c].replace('', 0)
            except Exception as e:
                print(station, c, e)

        weather.to_csv(os.path.join(OUTPUT_DIR, f"{station[0]:05}") + ".csv", index=False)
        
    print("Done!")
