import dlt
import yfinance as yf
import logging
from typing import Iterator, Dict
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from os import getenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv(dotenv_path="../.env")

# DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name="yahoo_finance_pipeline",
    destination="postgres",
    dataset_name="yahoo_finance",
    dev_mode=False
)

def get_stock_list() -> list:
    """Query the stock list from the Google Sheets staging table."""
    try:
        conn = psycopg2.connect(
            dbname=getenv("CREDENTIALS__DATABASE"), user=getenv("CREDENTIALS__USERNAME"), 
            password=getenv("CREDENTIALS__PASSWORD"), host=getenv("CREDENTIALS__HOST"), port=getenv("CREDENTIALS__PORT")
        )
        with conn.cursor() as cur:
            cur.execute("SELECT stock FROM google_sheets_staging.gsheet_finance")
            rows = cur.fetchall()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        logging.error(f"Failed to fetch stock list: {e}")
        return []

def fetch_yahoo_data(ticker: str) -> Dict:
    """Fetch data for a single stock ticker from Yahoo Finance."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        info['symbol'] = ticker  # Ensure ticker is included
        return info
    except Exception as e:
        logging.warning(f"Failed to fetch data for {ticker}: {e}")
        return {}

def extract_data() -> Iterator[Dict]:
    """Generator function that yields data for each stock."""
    tickers = get_stock_list()
    for ticker in tickers:
        data = fetch_yahoo_data(ticker)
        if data:
            yield data

def run():
    logging.info("Starting Yahoo Finance data extraction pipeline")
    try:
        load_info = pipeline.run(extract_data())
        logging.info(f"Pipeline run completed: {load_info}")
    except Exception as e:
        logging.error(f"Pipeline run failed: {e}")

if __name__ == "__main__":
    run()