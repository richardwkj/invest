import requests # type: ignore
import sqlite3
from datetime import datetime

API_KEY = '18NERQFXBESSATBF'  # Replace with your real key
BASE_URL = 'https://www.alphavantage.co/query'
symbol = 'AAPL'

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
    return data  ## I hadn't realized return is needed to actually output the data -_-

data = get_historical_data(symbol)

print(data)

## turns out this is a premium function aiyo
## {'Information': 'Thank you for using Alpha Vantage! This is a premium endpoint. You may subscribe to any of the premium plans at https://www.alphavantage.co/premium/ to instantly unlock all premium endpoints'}