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

    # 3. read raw JSON back
    with open(filename) as f:
        raw_data = json.load(f)

    # 4. transform JSON
    time_series = raw_data['Time Series (Daily)']
    df = pd.DataFrame.from_dict(time_series, orient='index')

    # rename columns
    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })

    # convert data types
    df = df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int
    })

    # reset index to make 'date' a column
    df = df.reset_index().rename(columns={"index": "date"})

    # sort by date ascending
    df = df.sort_values("date")

    # add daily change percentage
    df['daily_change_percentage'] = ((df['close'] - df['open']) / df['open']) * 100

    df.to_csv(f"{data_lake_folder}/{symbol}_{today_str}.csv", index=False)
    print(f"Processed {symbol}, first 5 rows:")
    print(df.head(), "\n")
