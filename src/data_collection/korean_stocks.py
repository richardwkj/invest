"""
Korean stock market data collection using yfinance package.
This module handles data collection from KOSPI and KOSDAQ markets via Yahoo Finance.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
import logging
import time
from pathlib import Path

from src.utils.logger import get_logger
from config.api_keys import get_api_key

logger = get_logger(__name__)


class KoreanStockCollector:
    """
    Collects Korean stock market data using yfinance package.
    
    This class handles data collection from KOSPI and KOSDAQ markets,
    including stock prices, market caps, and trading volumes.
    """
    
    def __init__(self):
        """Initialize the Korean stock collector."""
        self.logger = logger
        self.logger.info("Korean Stock Collector initialized with yfinance")
        
        # Common Korean stock suffixes
        self.kospi_suffix = ".KS"  # KOSPI stocks
        self.kosdaq_suffix = ".KQ"  # KOSDAQ stocks
        
        # Rate limiting
        self.request_delay = 0.1  # 100ms delay between requests
        
    def _add_suffix(self, symbol: str, market: str = "KOSPI") -> str:
        """
        Add appropriate suffix to stock symbol for Yahoo Finance.
        
        Args:
            symbol: Stock symbol (e.g., "005930")
            market: Market type ("KOSPI" or "KOSDAQ")
        
        Returns:
            Symbol with appropriate suffix (e.g., "005930.KS")
        """
        if market.upper() == "KOSDAQ":
            return f"{symbol}{self.kosdaq_suffix}"
        else:
            return f"{symbol}{self.kospi_suffix}"
    
    def get_stock_info(self, symbol: str, market: str = "KOSPI") -> Dict[str, Any]:
        """
        Get basic information about a stock.
        
        Args:
            symbol: Stock symbol (e.g., "005930")
            market: Market type ("KOSPI" or "KOSDAQ")
        
        Returns:
            Dictionary containing stock information
        """
        try:
            self.logger.info(f"Fetching stock info for {symbol} ({market})")
            
            # Add suffix for Yahoo Finance
            yf_symbol = self._add_suffix(symbol, market)
            
            # Get stock info
            stock = yf.Ticker(yf_symbol)
            info = stock.info
            
            # Add market information
            info['symbol'] = symbol
            info['market'] = market
            info['yf_symbol'] = yf_symbol
            
            # Clean up None values
            info = {k: v for k, v in info.items() if v is not None}
            
            self.logger.info(f"Retrieved info for {symbol}: {info.get('longName', 'Unknown')}")
            return info
            
        except Exception as e:
            self.logger.error(f"Error fetching stock info for {symbol}: {e}")
            return {}
    
    def get_stock_data(
        self, 
        symbol: str, 
        market: str = "KOSPI",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "1mo"
    ) -> pd.DataFrame:
        """
        Get historical data for a specific stock.
        
        Args:
            symbol: Stock symbol (e.g., "005930")
            market: Market type ("KOSPI" or "KOSDAQ")
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            period: Period if dates not specified ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
        
        Returns:
            DataFrame containing historical stock data
        """
        try:
            self.logger.info(f"Fetching historical data for {symbol} ({market})")
            
            # Add suffix for Yahoo Finance
            yf_symbol = self._add_suffix(symbol, market)
            
            # Get stock data
            stock = yf.Ticker(yf_symbol)
            
            if start_date and end_date:
                # Use specific date range
                df = stock.history(start=start_date, end=end_date)
            else:
                # Use period
                df = stock.history(period=period)
            
            # Add metadata
            df['symbol'] = symbol
            df['market'] = market
            df['yf_symbol'] = yf_symbol
            
            # Reset index to make Date a column
            df = df.reset_index()
            
            # Rate limiting
            time.sleep(self.request_delay)
            
            self.logger.info(f"Retrieved {len(df)} records for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_data(
        self, 
        symbols: List[str],
        market: str = "KOSPI",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "1mo"
    ) -> pd.DataFrame:
        """
        Get market-wide data for multiple stocks.
        
        Args:
            symbols: List of stock symbols
            market: Market type ("KOSPI" or "KOSDAQ")
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            period: Period if dates not specified
        
        Returns:
            DataFrame containing market data for all stocks
        """
        try:
            self.logger.info(f"Fetching market data for {len(symbols)} stocks from {market}")
            
            # Add suffixes for Yahoo Finance
            yf_symbols = [self._add_suffix(symbol, market) for symbol in symbols]
            
            # Get data for all symbols
            if start_date and end_date:
                df = yf.download(yf_symbols, start=start_date, end=end_date, progress=False)
            else:
                df = yf.download(yf_symbols, period=period, progress=False)
            
            # Rate limiting
            time.sleep(self.request_delay)
            
            self.logger.info(f"Retrieved market data for {len(symbols)} stocks")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return pd.DataFrame()
    
    def get_top_stocks(
        self, 
        market: str = "KOSPI",
        top_n: int = 50,
        sort_by: str = "marketCap"
    ) -> pd.DataFrame:
        """
        Get top stocks by market cap or other metrics.
        
        Args:
            market: Market type ("KOSPI" or "KOSDAQ")
            top_n: Number of top stocks to return
            sort_by: Sort metric ("marketCap", "volume", "price")
        
        Returns:
            DataFrame containing top stocks
        """
        try:
            self.logger.info(f"Fetching top {top_n} stocks from {market} by {sort_by}")
            
            # Get market symbols (this would need to be implemented based on available data)
            # For now, we'll use some common Korean stocks
            common_stocks = self._get_common_stocks(market)
            
            stock_data = []
            for symbol in common_stocks[:top_n]:
                info = self.get_stock_info(symbol, market)
                if info:
                    stock_data.append(info)
                time.sleep(self.request_delay)
            
            df = pd.DataFrame(stock_data)
            
            # Sort by specified metric
            if sort_by == "marketCap" and "marketCap" in df.columns:
                df = df.sort_values("marketCap", ascending=False)
            elif sort_by == "volume" and "volume" in df.columns:
                df = df.sort_values("volume", ascending=False)
            elif sort_by == "price" and "currentPrice" in df.columns:
                df = df.sort_values("currentPrice", ascending=False)
            
            # Return top N stocks
            top_stocks = df.head(top_n)
            
            self.logger.info(f"Retrieved top {len(top_stocks)} stocks by {sort_by}")
            return top_stocks
            
        except Exception as e:
            self.logger.error(f"Error fetching top stocks: {e}")
            return pd.DataFrame()
    
    def _get_common_stocks(self, market: str) -> List[str]:
        """
        Get list of common stock symbols for the specified market.
        
        Args:
            market: Market type ("KOSPI" or "KOSDAQ")
        
        Returns:
            List of stock symbols
        """
        if market.upper() == "KOSPI":
            return [
                "005930",  # Samsung Electronics
                "000660",  # SK Hynix
                "035420",  # NAVER
                "051910",  # LG Chem
                "006400",  # Samsung SDI
                "035720",  # Kakao
                "207940",  # Samsung Biologics
                "068270",  # Celltrion
                "323410",  # KakaoBank
                "373220",  # LG Energy Solution
                "005380",  # Hyundai Motor
                "000270",  # Kia
                "015760",  # Korea Electric Power
                "017670",  # SK Telecom
                "030200",  # KT
                "034020",  # Doosan
                "009150",  # Samsung C&T
                "028260",  # Samsung SDS
                "010130",  # Korea Zinc
                "096770",  # SK Innovation
            ]
        else:  # KOSDAQ
            return [
                "091990",  # Celltrion Healthcare
                "068760",  # Celltrion Pharm
                "086520",  # Ecopro
                "086520",  # Ecopro BM
                "091990",  # Celltrion Healthcare
                "068760",  # Celltrion Pharm
                "086520",  # Ecopro
                "086520",  # Ecopro BM
                "091990",  # Celltrion Healthcare
                "068760",  # Celltrion Pharm
            ]
    
    def get_market_summary(self, market: str = "KOSPI") -> Dict[str, Any]:
        """
        Get market summary information.
        
        Args:
            market: Market type ("KOSPI" or "KOSDAQ")
        
        Returns:
            Dictionary containing market summary
        """
        try:
            self.logger.info(f"Fetching market summary for {market}")
            
            # Get market index data
            if market.upper() == "KOSPI":
                index_symbol = "^KS11"  # KOSPI index
            else:
                index_symbol = "^KQ11"  # KOSDAQ index
            
            # Get index data
            index = yf.Ticker(index_symbol)
            info = index.info
            hist = index.history(period="5d")
            
            # Calculate summary statistics
            latest = hist.iloc[-1] if not hist.empty else None
            
            summary = {
                "market": market,
                "index_symbol": index_symbol,
                "current_price": latest['Close'] if latest is not None else None,
                "change": latest['Close'] - hist.iloc[-2]['Close'] if len(hist) > 1 else None,
                "change_percent": ((latest['Close'] - hist.iloc[-2]['Close']) / hist.iloc[-2]['Close'] * 100) if len(hist) > 1 else None,
                "volume": latest['Volume'] if latest is not None else None,
                "high": latest['High'] if latest is not None else None,
                "low": latest['Low'] if latest is not None else None,
                "open": latest['Open'] if latest is not None else None,
                "market_cap": info.get('marketCap'),
                "pe_ratio": info.get('trailingPE'),
                "dividend_yield": info.get('dividendYield'),
                "last_updated": datetime.now().isoformat()
            }
            
            self.logger.info(f"Retrieved market summary for {market}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error fetching market summary for {market}: {e}")
            return {}
    
    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filepath: Path to save the CSV file
        """
        try:
            # Create directory if it doesn't exist
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            self.logger.info(f"Data saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving data to {filepath}: {e}")
            raise
    
    def get_stock_list(self, market: str = "KOSPI") -> pd.DataFrame:
        """
        Get list of stocks in the specified market.
        Note: This is a simplified version using common stocks.
        For a complete list, you would need to maintain a database of all stocks.
        
        Args:
            market: Market type ("KOSPI" or "KOSDAQ")
        
        Returns:
            DataFrame containing stock information
        """
        try:
            self.logger.info(f"Fetching stock list for {market}")
            
            symbols = self._get_common_stocks(market)
            stock_data = []
            
            for symbol in symbols:
                info = self.get_stock_info(symbol, market)
                if info:
                    stock_data.append(info)
                time.sleep(self.request_delay)
            
            df = pd.DataFrame(stock_data)
            self.logger.info(f"Retrieved {len(df)} stocks from {market}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching stock list for {market}: {e}")
            return pd.DataFrame()


# Convenience functions
def get_korean_stocks(market: str = "KOSPI") -> pd.DataFrame:
    """Get list of Korean stocks."""
    collector = KoreanStockCollector()
    return collector.get_stock_list(market)

def get_stock_history(symbol: str, market: str = "KOSPI", start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Get historical data for a specific stock."""
    collector = KoreanStockCollector()
    return collector.get_stock_data(symbol, market, start_date, end_date)

def get_market_summary(market: str = "KOSPI") -> Dict[str, Any]:
    """Get market summary information."""
    collector = KoreanStockCollector()
    return collector.get_market_summary(market)

def get_top_stocks(market: str = "KOSPI", top_n: int = 50) -> pd.DataFrame:
    """Get top stocks by market cap."""
    collector = KoreanStockCollector()
    return collector.get_top_stocks(market, top_n) 