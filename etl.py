import requests
import json
import pandas as pd
import os
import time
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# parameters
API_KEY = "7POU20Z8FG2KJPU1"
symbols = ["AAPL", "GOOG", "MSFT"]  # companies to fetch
data_lake_folder = "raw_data"
FETCH_FROM_API = True  # change to false to skip api calls


# make sure folder exists
os.makedirs(data_lake_folder, exist_ok=True)

# save raw JSON into data lake
today_str = date.today().isoformat()

for symbol in symbols:
    filename = f"{data_lake_folder}/{symbol}_{today_str}.json"

    # skip api call if file already exists
    if os.path.exists(filename):
        print(f"📁 Skipping API fetch for {symbol}, file already exists.")
        # read raw JSON back
        with open(filename) as f:
            raw_data = json.load(f)
    elif FETCH_FROM_API:
        print(f"🌐 Fetching data for {symbol} from Alpha Vantage...")
        # fetch data from API
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
        response = requests.get(url)
        data = response.json()

        # check for api error
        if 'Time Series (Daily)' not in raw_data:
            print(f"Error fetching {symbol}:")
            print(raw_data)
            continue

        # save raw json into data lake
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)


        raw_data = data
        print(f"✅ Saved {symbol} data to {filename}")

        # sleep to avoid hitting the 5 requests/minute limit
        print("Waiting 15 seconds to respect rate limit...\n")
        time.sleep(15)
    else:
        print(f"⚠️ FETCH_FROM_API=False — reading local {symbol} data.")
        if not os.path.exists(filename):
            print(f"❌ No local file found for {symbol}, skipping.")
            continue
        with open(filename) as f:
            raw_data = json.load(f)

    # transform json
    if 'Time Series (Daily)' not in raw_data:
        print(f"⚠️ Skipping {symbol}, invalid data structure.")
        continue


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
    df = df.sort_values("date")

    # add daily change percentage
    df['daily_change_percentage'] = ((df['close'] - df['open']) / df['open']) * 100

    print(f"Processed {symbol}, first 3 rows:")
    print(df.head(3), "\n")



# loads variables from .env
load_dotenv()

# read from environment
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
host = os.getenv("DB_HOST")
port = int(os.getenv("DB_PORT", 3306))
database = os.getenv("DB_NAME")

# Safety check for DB connection
if not all([username, password, host, database]):
    print("❌ Missing database credentials. Check your .env file.")
    print(f"DB_USER={username}, DB_PASS={bool(password)}, DB_HOST={host}, DB_NAME={database}")
else:
    engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")


    query = """
        CREATE TABLE IF NOT EXISTS stock_daily_data (
        id INT PRIMARY KEY AUTO_INCREMENT,
        symbol VARCHAR(10),
        date DATE,
        open_price DECIMAL(15,4),
        high_price DECIMAL(15,4),
        low_price DECIMAL(15,4),
        close_price DECIMAL(15,4),
        volume INT,
        daily_change_percentage DECIMAL(10,4),
        extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()  # make sure changes are saved

    print("✅ Table ensured (created if not existed).")
