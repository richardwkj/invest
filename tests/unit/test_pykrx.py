from pykrx import stock
from pykrx import bond
import time

tickers = stock.get_market_ticker_list("20190225", market="KOSDAQ")

for ticker in stock.get_stock_ticker_list():
    df = stock.get_market_ohlcv("20181210", "20181212", ticker)
    print(df.head())
    time.sleep(1)

