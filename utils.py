import aiohttp
import asyncio
from pathlib import Path
import os

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

async def download_files(name, urls, dates, processor):
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