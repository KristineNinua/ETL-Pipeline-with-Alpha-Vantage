import requests
import json
import pandas as pd
import os
from datetime import date

# parameters
API_KEY = "7POU20Z8FG2KJPU1"
symbols = ["AAPL", "GOOG", "MSFT"]  # companies to fetch
data_lake_folder = "raw_data"

# making sure folder exists
os.makedirs(data_lake_folder, exist_ok=True)

for symbol in symbols:
    # 1. fetch data from API
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    # 2. save raw JSON into data lake
    today_str = date.today().isoformat()  # YYYY-MM-DD
    filename = f"{data_lake_folder}/{symbol}_{today_str}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

    # 3. Read raw JSON back
    with open(filename) as f:
        raw_data = json.load(f)

    # 4. Transform JSON into Pandas DataFrame
    time_series = raw_data['Time Series (Daily)']
    df = pd.DataFrame.from_dict(time_series, orient='index')
