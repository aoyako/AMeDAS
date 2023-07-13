import aiohttp
import asyncio
import os
from datetime import datetime
import pandas as pd
from pathlib import Path
from dateutil import relativedelta
import sys

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
    with open(file, 'r', errors='ignore') as f:
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

def merge_csvs(files, name):
    df = pd.DataFrame()
    for file in files:
        tmp_df = pd.read_csv(file)
        df = pd.concat([df, tmp_df], axis=0)
    
    df.to_csv(name+"_rad.csv", index=False)

def format_csv(file, date):
    df = pd.read_csv(file)
    df["month"] = date.month
    df["year"] = date.year
    df["day"] = list(range(1, len(df)+1))
    
    df.to_csv(file, index=False)

async def download_files(name, urls, dates):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5), timeout=10000) as session:
        tasks = []
        filenames = [os.path.join(CSV_DIR, f"{l}.csv") for l in range(len(urls))]
        for i in range(len(urls)):
            task = asyncio.create_task(download_file(session, urls[i], filenames[i]))
            tasks.append(task)
        await asyncio.gather(*tasks)
        await asyncio.sleep(1)

        for file in filenames:
            extract_csv(file)
        for i, file in enumerate(filenames):
            format_csv(file, dates[i])
        merge_csvs(filenames, name)


def get_download_url_dl(name, year, month):
    return f"https://www.data.jma.go.jp/gmd/env/radiation/data/geppo/{year}{month:02}/DL{year}{month:02}_{name}.txt"

def get_download_url_df(name, year, month):
    return f"https://www.data.jma.go.jp/gmd/env/radiation/data/geppo/{year}{month:02}/DF{year}{month:02}_{name}.txt"

def get_download_url_dr(name, year, month):
    return f"https://www.data.jma.go.jp/gmd/env/radiation/data/geppo/{year}{month:02}/DR{year}{month:02}_{name}.txt"


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

    stations_df = pd.read_csv("stations.csv")
    station_ids = stations_df["id"].to_numpy()
    station_codes = [str(code)[:2] for code in stations_df["code"].to_numpy()]

    stations = list(zip(station_ids, station_codes))

    urls = []

    loop = asyncio.get_event_loop()
    names = {
        "47409": "abs",
        "47646": "tat",
        "47807": "fua",
    }
    for station in stations:
        name = names[f"{station[0]:05}"]
        urls = [get_download_url_dl(name, date.year, date.month) for date in download_dates]
        loop.run_until_complete(download_files(os.path.join(OUTPUT_DIR, f"{station[0]:05}" + "_DR"), urls, download_dates))
        print(f"Downloaded station DIRECT SOLAR RADIATION {station[0]}")
        
        urls = [get_download_url_dl(name, date.year, date.month) for date in download_dates]
        loop.run_until_complete(download_files(os.path.join(OUTPUT_DIR, f"{station[0]:05}" + "_DF"), urls, download_dates))
        print(f"Downloaded station DIFFUSE SOLAR RADIATION {station[0]}")
        
        urls = [get_download_url_dl(name, date.year, date.month) for date in download_dates]
        loop.run_until_complete(download_files(os.path.join(OUTPUT_DIR, f"{station[0]:05}" + "_DL"), urls, download_dates))
        print(f"Downloaded station DOWNWARD LONGWAVE RADIATION {station[0]}")
        
    print("Done!")