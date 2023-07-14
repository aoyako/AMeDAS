import aiohttp
import asyncio
from pathlib import Path
import os
from typing import List
from datetime import datetime
import logging

CSV_DIR = "csv"
OUTPUT_DIR = "output"
Path(CSV_DIR).mkdir(parents=True, exist_ok=True)
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

STATIONS = "stations.csv"

class Processor():
    @staticmethod
    def extract_csv(file: str):
        pass
    @staticmethod
    def format_csv(file: str, dates: List[datetime]):
        pass
    @staticmethod
    def merge_csvs(files: List[str]):
        pass

async def download_file(session: aiohttp.ClientSession, url: str, name: str):
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
            logging.exception(e)

async def download_files(name: str, urls: List[str], dates: List[datetime], processor: Processor):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5), timeout=10000) as session:
        tasks = []
        filenames = [os.path.join(CSV_DIR, f"{l}.csv") for l in range(len(urls))]
        for i in range(len(urls)):
            task = asyncio.create_task(download_file(session, urls[i], filenames[i]))
            tasks.append(task)
        await asyncio.gather(*tasks)
        await asyncio.sleep(1)

        for file in filenames:
            processor.extract_csv(file)
        for i, file in enumerate(filenames):
            processor.format_csv(file, dates[i])
        processor.merge_csvs(filenames, name)