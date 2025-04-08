# This script fetches the current stock price of a given symbol using the Alpha Vantage API.
# It requires the 'requests' library to make HTTP requests.
# You can install it using pip if you haven't already:
# pip install requests

# Ensure the 'requests' library is installed by running the following command in your terminal:
# pip install requests
# Import the requests library to make HTTP requests

import requests # type: ignore
import sqlite3
from datetime import datetime

API_KEY = '18NERQFXBESSATBF'  # Replace with your real key
BASE_URL = 'https://www.alphavantage.co/query'

def get_stock_price(symbol):
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if "Global Quote" in data:
        quote = data["Global Quote"]
        print(f"Symbol: {quote['01. symbol']}")
        print(f"Price: ${quote['05. price']}")
        print(f"Change: {quote['09. change']} ({quote['10. change percent']})")
    else:
        print("Error fetching data. Please check the symbol and try again.")



# get_stock_price('AAPL')

def get_historical_data(symbol):
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': symbol,
        'outputsize': 'full',  # 'compact' for last 100 data points, 'full' for full-length
        'apikey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if 'Time Series (Daily)' in data:
        return data['Time Series (Daily)']
    else:
        print("Error fetching historical data")
        return None

def save_to_db(symbol, time_series):
    # Connect to SQLite DB (creates it if it doesn't exist)
    conn = sqlite3.connect('stocks.db')
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adjusted_close REAL,
            volume INTEGER,
            PRIMARY KEY (symbol, date)
        )
    ''')

    # Insert historical data into DB
    for date_str, daily_data in time_series.items():
        try:
            cursor.execute(f'''
                INSERT OR REPLACE INTO stock_data (
                    symbol, date, open, high, low, close, adjusted_close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                date_str,
                float(daily_data['1. open']),
                float(daily_data['2. high']),
                float(daily_data['3. low']),
                float(daily_data['4. close']),
                float(daily_data['5. adjusted close']),
                int(daily_data['6. volume'])
            ))
        except Exception as e:
            print(f"Error inserting data for {date_str}: {e}")

    conn.commit()
    conn.close()


if __name__ == '__main__':
    symbol = input("Enter stock symbol (e.g., AAPL): ").upper()
    time_series = get_historical_data(symbol)
    if time_series:
        save_to_db(symbol, time_series)
        print(f"Historical data for {symbol} saved to database.")