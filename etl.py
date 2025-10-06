import requests
import json
import pandas as pd
import os
import time
import schedule
from datetime import datetime, date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect, exc
from pydantic import BaseModel, Field, RootModel, ValidationError
from typing import Dict, Any, List


# Pydantic Data Validation Models
class DailyStockData(BaseModel):
    open: float = Field(alias="1. open")
    high: float = Field(alias="2. high")
    low: float = Field(alias="3. low")
    close: float = Field(alias="4. close")
    volume: int = Field(alias="5. volume")


class TimeSeriesDaily(RootModel[Dict[str, DailyStockData]]):
    pass


class AlphaVantageResponse(BaseModel):
    meta_data: Dict[str, Any] = Field(alias="Meta Data")
    time_series_daily: TimeSeriesDaily = Field(alias="Time Series (Daily)")


# ETL Pipeline Functions
def extract_and_transform(api_key: str, symbols: List[str], data_lake_folder: str, fetch_from_api: bool = True) -> pd.DataFrame:
    os.makedirs(data_lake_folder, exist_ok=True)
    today_str = date.today().isoformat()
    all_data = []

    for symbol in symbols:
        filename = f"{data_lake_folder}/{symbol}_{today_str}.json"

        # Extract
        if os.path.exists(filename):
            print(f"📁 Skipping API fetch for {symbol}, file already exists.")
            with open(filename) as f:
                raw_data = json.load(f)
        elif fetch_from_api:
            print(f"🌐 Fetching data for {symbol} from Alpha Vantage...")
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
            response = requests.get(url)
            data = response.json()

            try:
                AlphaVantageResponse(**data)
            except ValidationError as e:
                print(f"❌ Validation error for {symbol}: {e}")
                continue

            with open(filename, "w") as f:
                json.dump(data, f, indent=4)
            raw_data = data
            print(f"✅ Saved {symbol} data to {filename}")
            print("⏳ Waiting 15 seconds to respect rate limit...\n")
            time.sleep(15)
        else:
            print(f"⚠️ FETCH_FROM_API=False — reading local {symbol} data.")
            if not os.path.exists(filename):
                print(f"❌ No local file found for {symbol}, skipping.")
                continue
            with open(filename) as f:
                raw_data = json.load(f)

        # Validate loaded data
        try:
            validated_data = AlphaVantageResponse(**raw_data)
        except ValidationError as e:
            print(f"❌ Validation error while processing {symbol}: {e}")
            continue

        # Transform
        time_series = validated_data.time_series_daily.root
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        })
        df = df.astype({
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": int
        })
        df = df.reset_index().rename(columns={"index": "date"})
        df = df.sort_values("date")
        df['daily_change_percentage'] = ((df['close'] - df['open']) / df['open']) * 100
        df['symbol'] = symbol

        all_data.append(df)
        print(f"Processed {symbol}, first 3 rows:\n{df.head(3)}\n")

    if not all_data:
        print("⚠️ No data extracted. Returning empty DataFrame.")
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)


def load_to_db(df: pd.DataFrame, engine, symbols: List[str]):
    if df.empty:
        print("⚠️ Empty DataFrame, nothing to insert.")
        return

    create_table_query = """
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
        extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (symbol, date)
    )
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_table_query))
        print("✅ Table ensured (created if not existed).")
    except exc.SQLAlchemyError as e:
        print(f"❌ Database error while creating table: {e}")
        return

    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                insert_query = text("""
                    INSERT IGNORE INTO stock_daily_data
                    (symbol, date, open_price, high_price, low_price, close_price, volume, daily_change_percentage)
                    VALUES (:symbol, :date, :open, :high, :low, :close, :volume, :daily_change_percentage)
                """)
                conn.execute(insert_query, {
                    "symbol": row["symbol"],
                    "date": row["date"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "daily_change_percentage": row["daily_change_percentage"]
                })
        print(f"📤 Inserted {len(df)} rows (ignoring duplicates) for {len(symbols)} symbols.\n")
    except exc.SQLAlchemyError as e:
        print(f"❌ Database error while inserting data: {e}")


def inspect_database(engine):
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print("\n📋 Tables in database:", tables)

        if "stock_daily_data" in tables:
            print("✅ 'stock_daily_data' table exists!")
            columns = inspector.get_columns("stock_daily_data")
            print("\n📑 Columns:")
            for col in columns:
                print(f"{col['name']} ({col['type']})")

            df_preview = pd.read_sql("SELECT * FROM stock_daily_data ORDER BY id DESC LIMIT 5", engine)
            print("\n🧾 Last 5 inserted rows:")
            print(df_preview)
        else:
            print("❌ 'stock_daily_data' table not found.")
    except exc.SQLAlchemyError as e:
        print(f"❌ Database inspection error: {e}")


def main():
    load_dotenv()
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT", 3306))
    database = os.getenv("DB_NAME")

    if not all([username, password, host, database]):
        print("❌ Missing database credentials. Check your .env file.")
        return

    try:
        engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")
        print("✅ Database connection established.")
    except exc.SQLAlchemyError as e:
        print(f"❌ Database connection failed: {e}")
        return

    try:
        print("\n🚀 Starting ETL Pipeline...\n")
        api_key = "LP89JFY0HI9OE8Z8"
        symbols = ["AAPL", "GOOG", "MSFT"]
        data_lake_folder = "raw_data"
        final_df = extract_and_transform(api_key, symbols, data_lake_folder, fetch_from_api=True)
        load_to_db(final_df, engine, symbols)
        inspect_database(engine)
        print("\n🎉 ETL pipeline completed successfully.")
    except Exception as e:
        print(f"❌ Unexpected error during ETL: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()
            print("🔌 Database connection closed.")


def job():
    print(f"\n🕘 Running scheduled ETL job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    main()
    with open("etl_log.txt", "a") as log:
        log.write(f"ETL run at {datetime.now()}\n")


# Schedule the job
if __name__ == "__main__":
    schedule.every().day.at("09:00").do(job)
    print("📅 Scheduler started. Waiting for next run...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🛑 Scheduler stopped manually.")

# if __name__ == "__main__":
#     job()  # run immediately once
