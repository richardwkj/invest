# This script fetches the current stock price of a given symbol using the Alpha Vantage API.
# It requires the 'requests' library to make HTTP requests.
# You can install it using pip if you haven't already:
# pip install requests

# Ensure the 'requests' library is installed by running the following command in your terminal:
# pip install requests
# Import the requests library to make HTTP requests

import requests # type: ignore

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

if __name__ == '__main__':
    symbol = input("Enter stock symbol (e.g., AAPL): ").upper()
    time_series = get_historical_data(symbol)
    if time_series:
        save_to_db(symbol, time_series)
        print(f"Historical data for {symbol} saved to database.")