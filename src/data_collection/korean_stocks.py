"""
Korean stock market data collection using marcap package.
This module handles data collection from KOSPI and KOSDAQ markets.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging

from src.utils.logger import get_logger
from config.api_keys import get_api_key

logger = get_logger(__name__)

try:
    import marcap
    MARCAP_AVAILABLE = True
except ImportError:
    MARCAP_AVAILABLE = False
    logger.warning("marcap package not available. Install with: pip install marcap")


class KoreanStockCollector:
    """
    Collects Korean stock market data using marcap package.
    
    This class handles data collection from KOSPI and KOSDAQ markets,
    including stock prices, market caps, and trading volumes.
    """
    
    def __init__(self):
        """Initialize the Korean stock collector."""
        if not MARCAP_AVAILABLE:
            raise ImportError("marcap package is required for Korean stock data collection")
        
        self.logger = logger
        self.logger.info("Korean Stock Collector initialized")
    
    def get_stock_list(self, market: str = "ALL") -> pd.DataFrame:
        """
        Get list of all stocks in the specified market.
        
        Args:
            market: Market to filter by ("KOSPI", "KOSDAQ", "ALL")
        
        Returns:
            DataFrame containing stock information
        """
        try:
            self.logger.info(f"Fetching stock list for market: {market}")
            
            # Get current date
            today = datetime.now().date()
            
            # Fetch stock data for today
            df = marcap.marcap_data(today, today)
            
            # Filter by market if specified
            if market.upper() == "KOSPI":
                df = df[df['Market'] == 'KOSPI']
            elif market.upper() == "KOSDAQ":
                df = df[df['Market'] == 'KOSDAQ']
            elif market.upper() != "ALL":
                self.logger.warning(f"Unknown market: {market}. Using ALL markets.")
            
            # Clean and standardize column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            self.logger.info(f"Retrieved {len(df)} stocks from {market} market")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching stock list: {e}")
            raise
    
    def get_stock_data(
        self, 
        symbol: str, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get historical data for a specific stock.
        
        Args:
            symbol: Stock symbol/ticker
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
        
        Returns:
            DataFrame containing historical stock data
        """
        try:
            self.logger.info(f"Fetching historical data for {symbol}")
            
            # Set default dates if not provided
            if not end_date:
                end_date = datetime.now().date()
            if not start_date:
                start_date = end_date - timedelta(days=30)  # Default to 30 days
            
            # Convert string dates to datetime
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # Fetch historical data
            df = marcap.marcap_data(start_date, end_date, symbol)
            
            # Clean and standardize column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            # Add symbol column if not present
            if 'symbol' not in df.columns:
                df['symbol'] = symbol
            
            self.logger.info(f"Retrieved {len(df)} records for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise
    
    def get_market_data(
        self, 
        market: str = "ALL",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get market-wide data for all stocks in the specified market.
        
        Args:
            market: Market to fetch data for ("KOSPI", "KOSDAQ", "ALL")
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
        
        Returns:
            DataFrame containing market data
        """
        try:
            self.logger.info(f"Fetching market data for {market}")
            
            # Set default dates if not provided
            if not end_date:
                end_date = datetime.now().date()
            if not start_date:
                start_date = end_date - timedelta(days=7)  # Default to 7 days
            
            # Convert string dates to datetime
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # Fetch market data
            df = marcap.marcap_data(start_date, end_date)
            
            # Filter by market if specified
            if market.upper() == "KOSPI":
                df = df[df['Market'] == 'KOSPI']
            elif market.upper() == "KOSDAQ":
                df = df[df['Market'] == 'KOSDAQ']
            elif market.upper() != "ALL":
                self.logger.warning(f"Unknown market: {market}. Using ALL markets.")
            
            # Clean and standardize column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            self.logger.info(f"Retrieved {len(df)} market records for {market}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            raise
    
    def get_top_stocks(
        self, 
        market: str = "ALL",
        metric: str = "market_cap",
        top_n: int = 50
    ) -> pd.DataFrame:
        """
        Get top stocks by specified metric.
        
        Args:
            market: Market to filter by ("KOSPI", "KOSDAQ", "ALL")
            metric: Metric to sort by ("market_cap", "volume", "price")
            top_n: Number of top stocks to return
        
        Returns:
            DataFrame containing top stocks
        """
        try:
            self.logger.info(f"Fetching top {top_n} stocks by {metric} from {market}")
            
            # Get current stock list
            df = self.get_stock_list(market)
            
            # Sort by metric
            if metric.lower() == "market_cap":
                df = df.sort_values('market_cap', ascending=False)
            elif metric.lower() == "volume":
                df = df.sort_values('volume', ascending=False)
            elif metric.lower() == "price":
                df = df.sort_values('close', ascending=False)
            else:
                self.logger.warning(f"Unknown metric: {metric}. Using market_cap.")
                df = df.sort_values('market_cap', ascending=False)
            
            # Return top N stocks
            top_stocks = df.head(top_n)
            
            self.logger.info(f"Retrieved top {len(top_stocks)} stocks by {metric}")
            return top_stocks
            
        except Exception as e:
            self.logger.error(f"Error fetching top stocks: {e}")
            raise
    
    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filepath: Path to save the CSV file
        """
        try:
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            self.logger.info(f"Data saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving data to {filepath}: {e}")
            raise


# Convenience functions
def get_korean_stocks(market: str = "ALL") -> pd.DataFrame:
    """Get list of Korean stocks."""
    collector = KoreanStockCollector()
    return collector.get_stock_list(market)

def get_stock_history(symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Get historical data for a specific stock."""
    collector = KoreanStockCollector()
    return collector.get_stock_data(symbol, start_date, end_date)

def get_market_history(market: str = "ALL", start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Get market-wide historical data."""
    collector = KoreanStockCollector()
    return collector.get_market_data(market, start_date, end_date) 