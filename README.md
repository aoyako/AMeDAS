# AMeDAS
Script to download daily statistics from AMeDAS website.

**The Automated Meteorological Data Acquisition System (AMeDAS)**\
https://www.jma.go.jp/jma/en/Activities/amedas/amedas.html

### Prerequisites
```bash
python3 -m venv amedas
python3 -m pip install -r requirements.txt
```

## Weather
### Running
Put desired stations in `stations.csv` file.\
List of possible stations you can find in `stations_list.csv`
```bash
python3 main.py <start_date> <end_date>
```
Check folder `output`


### Example
```bash
python3 main.py 2021/01/24 2022/01/25
ls -l output/
```
