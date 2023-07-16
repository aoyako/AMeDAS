import os
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import sys
import radiation as rad
import weather as wr
import utils
from typing import List
from datetime import datetime
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    mode = sys.argv[1]
    begin_day_string = sys.argv[2]
    end_day_string = sys.argv[3]
    
    if mode not in ["daily", "hourly", "10min"]:
        logging.error("mode must be in [\"daily\", \"hourly\", \"10min\"]")
        os._exit(1)
    
    begin_day = datetime.strptime(begin_day_string, "%Y/%m/%d")
    end_day = datetime.strptime(end_day_string, "%Y/%m/%d")

    stations_df = pd.read_csv(utils.STATIONS)
    station_ids = stations_df["id"].to_numpy()
    station_codes = [str(code)[:2] for code in stations_df["code"].to_numpy()]

    stations = list(zip(station_ids, station_codes))

    for station in stations:
        weather = wr.get_station(station, begin_day, end_day, mode)        
        if mode == "daily":
            radiation = rad.get_station(f"{station[0]:05}", begin_day, end_day)
            weather = pd.merge(weather, radiation, on=["year", "month", "day"], how="outer")
        logging.info(f"Downloaded station {station[0]}")

        weather.to_csv(os.path.join(utils.OUTPUT_DIR, f"{station[0]:05}") + ".csv", index=False)
        
    logging.info("Done!")
