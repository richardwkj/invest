"""
Korean stock market data collection using pykrx and yfinance packages.
This module handles data collection from KOSPI and KOSDAQ markets with PostgreSQL storage.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
import logging
import time
from pathlib import Path
import requests
import json

# Database imports
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# pykrx for Korean stock data
from pykrx import stock

from src.utils.logger import get_logger
from config.api_keys import get_api_key

logger = get_logger(__name__)

# Database setup
Base = declarative_base()

class KoreanStock(Base):
    """PostgreSQL table for Korean stock data."""
    __tablename__ = 'korean_stocks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    market = Column(String(10), nullable=False)  # KOSPI or KOSDAQ
    longname = Column(Text, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    open_price = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(Integer, nullable=True)
    dividends = Column(Float, nullable=True)
    stock_splits = Column(Float, nullable=True)
    date = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class KoreanStockCollector:
    """
    Collects Korean stock market data using pykrx and yfinance packages.
    
    This class handles data collection from KOSPI and KOSDAQ markets,
    including stock prices, market caps, and trading volumes with PostgreSQL storage.
    """
    
    def __init__(self, database_url: str = None):
        """Initialize the Korean stock collector."""
        self.logger = logger
        self.logger.info("Korean Stock Collector initialized with pykrx and PostgreSQL")
        
        # Rate limiting
        self.request_delay = 3.0  # 3 seconds delay between API calls
        
        # Common Korean stock suffixes
        self.kospi_suffix = ".KS"  # KOSPI stocks
        self.kosdaq_suffix = ".KQ"  # KOSDAQ stocks
        
        # Database setup
        if database_url:
            self.database_url = database_url
        else:
            # Default PostgreSQL connection
            self.database_url = "postgresql://username:password@localhost:5432/korean_stocks"
        
        self.engine = create_engine(self.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables
        self._create_tables()
        
    def _create_tables(self):
        """Create database tables if they don't exist."""
        try:
            Base.metadata.create_all(bind=self.engine)
            self.logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating database tables: {e}")
            raise
    
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
    
    def get_all_stock_codes_from_pykrx(self) -> List[Dict[str, Any]]:
        """
        Step 1: Access pykrx to get the list of all stock codes.
        
        Returns:
            List of dictionaries containing stock information from pykrx
        """
        try:
            self.logger.info("Fetching all stock codes from pykrx")
            
            # Get all stock tickers from pykrx
            tickers = stock.get_market_ticker_list()
            
            if not tickers:
                self.logger.error("No stock tickers found from pykrx")
                return []
            
            # Get detailed information for each ticker
            companies = []
            for ticker in tickers:
                try:
                    # Get stock name
                    name = stock.get_market_ticker_name(ticker)
                    
                    # Determine market based on ticker pattern
                    # KOSPI typically has codes starting with 0, KOSDAQ with other numbers
                    market = "KOSPI" if ticker.startswith('0') else "KOSDAQ"
                    
                    company_info = {
                        'corp_name': name,
                        'stock_code': ticker,
                        'market': market
                    }
                    companies.append(company_info)
                    
                except Exception as e:
                    self.logger.warning(f"Error getting info for ticker {ticker}: {e}")
                    continue
            
            self.logger.info(f"Retrieved {len(companies)} companies from pykrx")
            return companies
            
        except Exception as e:
            self.logger.error(f"Error fetching data from pykrx: {e}")
            # Use fallback when pykrx fails
            self.logger.warning("Using fallback stock list due to pykrx error")
            return self._get_fallback_stock_list()
    
    def _get_fallback_stock_list(self) -> List[Dict[str, Any]]:
        """
        Fallback method to get a sample list of Korean stocks for testing.
        Used when pykrx is not available or fails.
        
        Returns:
            List of dictionaries containing stock information
        """
        fallback_stocks = [
            {
                'corp_name': 'Samsung Electronics Co., Ltd.',
                'stock_code': '005930',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'SK Hynix Inc.',
                'stock_code': '000660',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'NAVER Corp.',
                'stock_code': '035420',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'LG Chem Ltd.',
                'stock_code': '051910',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'Samsung SDI Co., Ltd.',
                'stock_code': '006400',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'Kakao Corp.',
                'stock_code': '035720',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'Samsung Biologics Co., Ltd.',
                'stock_code': '207940',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'Celltrion Inc.',
                'stock_code': '068270',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'KakaoBank Corp.',
                'stock_code': '323410',
                'market': 'KOSPI'
            },
            {
                'corp_name': 'LG Energy Solution Ltd.',
                'stock_code': '373220',
                'market': 'KOSPI'
            }
        ]
        
        self.logger.info(f"Using fallback stock list with {len(fallback_stocks)} companies")
        return fallback_stocks
    
    def get_stock_data_with_max_history(self, symbol: str, market: str = "KOSPI") -> pd.DataFrame:
        """
        Step 2: Get maximum historical data for a specific stock with 1-day intervals.
        
        Args:
            symbol: Stock symbol (e.g., "005930")
            market: Market type ("KOSPI" or "KOSDAQ")
        
        Returns:
            DataFrame containing historical stock data
        """
        try:
            self.logger.info(f"Fetching max historical data for {symbol} ({market})")
            
            # Add suffix for Yahoo Finance
            yf_symbol = self._add_suffix(symbol, market)
            
            # Get stock data with maximum history
            stock_obj = yf.Ticker(yf_symbol)
            df = stock_obj.history(period="max", interval="1d")
            
            if df.empty:
                self.logger.warning(f"No data found for {symbol} ({market})")
                return pd.DataFrame()
            
            # Add metadata
            df['symbol'] = symbol
            df['market'] = market
            df['yf_symbol'] = yf_symbol
            
            # Reset index to make Date a column
            df = df.reset_index()
            
            # Rename columns to match database schema
            df = df.rename(columns={
                'Open': 'open_price',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Dividends': 'dividends',
                'Stock Splits': 'stock_splits',
                'Date': 'date'
            })
            
            # Get company name
            try:
                info = stock_obj.info
                longname = info.get('longName', 'Unknown')
                df['longname'] = longname
            except:
                df['longname'] = 'Unknown'
            
            self.logger.info(f"Retrieved {len(df)} records for {symbol} ({market})")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol} ({market}): {e}")
            return pd.DataFrame()
    
    def collect_all_stocks_data(self) -> pd.DataFrame:
        """
        Step 3: Collect data for all stocks with proper rate limiting.
        
        Returns:
            DataFrame containing all collected stock data
        """
        try:
            self.logger.info("Starting comprehensive stock data collection")
            
            # Step 1: Get all stock codes from pykrx
            companies = self.get_all_stock_codes_from_pykrx()
            
            if not companies:
                self.logger.error("No companies found from pykrx")
                return pd.DataFrame()
            
            all_stock_data = []
            
            # Step 2: Process each company
            for i, company in enumerate(companies):
                try:
                    stock_code = company.get('stock_code', '').strip()
                    corp_name = company.get('corp_name', '').strip()
                    market = company.get('market', 'KOSPI')
                    
                    if not stock_code or len(stock_code) != 6:
                        continue
                    
                    self.logger.info(f"Processing {i+1}/{len(companies)}: {corp_name} ({stock_code}) - {market}")
                    
                    # Get stock data with max history
                    stock_df = self.get_stock_data_with_max_history(stock_code, market)
                    
                    if not stock_df.empty:
                        # Add company name from pykrx
                        stock_df['longname'] = corp_name
                        all_stock_data.append(stock_df)
                        
                        # Save to database
                        self.save_stock_data_to_db(stock_df)
                    
                    # Step 4: 3-second pause to prevent API overloading
                    time.sleep(self.request_delay)
                    
                except Exception as e:
                    self.logger.error(f"Error processing company {company.get('corp_name', 'Unknown')}: {e}")
                    continue
            
            # Combine all data
            if all_stock_data:
                combined_df = pd.concat(all_stock_data, ignore_index=True)
                self.logger.info(f"Total records collected: {len(combined_df)}")
                return combined_df
            else:
                self.logger.warning("No stock data collected")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error in comprehensive data collection: {e}")
            return pd.DataFrame()
    
    def save_stock_data_to_db(self, df: pd.DataFrame) -> bool:
        """
        Save stock data to PostgreSQL database.
        
        Args:
            df: DataFrame containing stock data
        
        Returns:
            True if successful, False otherwise
        """
        try:
            session = self.SessionLocal()
            
            for _, row in df.iterrows():
                # Check if record already exists
                existing = session.query(KoreanStock).filter(
                    KoreanStock.symbol == row['symbol'],
                    KoreanStock.date == row['date']
                ).first()
                
                if existing:
                    # Update existing record
                    existing.high = row.get('high')
                    existing.low = row.get('low')
                    existing.open_price = row.get('open_price')
                    existing.close = row.get('close')
                    existing.volume = row.get('volume')
                    existing.dividends = row.get('dividends')
                    existing.stock_splits = row.get('stock_splits')
                    existing.longname = row.get('longname')
                else:
                    # Create new record
                    stock_record = KoreanStock(
                        symbol=row['symbol'],
                        market=row['market'],
                        longname=row.get('longname', 'Unknown'),
                        high=row.get('high'),
                        low=row.get('low'),
                        open_price=row.get('open_price'),
                        close=row.get('close'),
                        volume=row.get('volume'),
                        dividends=row.get('dividends'),
                        stock_splits=row.get('stock_splits'),
                        date=row['date']
                    )
                    session.add(stock_record)
            
            session.commit()
            session.close()
            
            self.logger.info(f"Saved {len(df)} records to database")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error: {e}")
            session.rollback()
            session.close()
            return False
        except Exception as e:
            self.logger.error(f"Error saving to database: {e}")
            session.rollback()
            session.close()
            return False
    
    def get_stock_data_from_db(self, symbol: str = None, market: str = None, 
                              start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Retrieve stock data from PostgreSQL database.
        
        Args:
            symbol: Stock symbol to filter by
            market: Market to filter by (KOSPI or KOSDAQ)
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
        
        Returns:
            DataFrame containing stock data from database
        """
        try:
            session = self.SessionLocal()
            query = session.query(KoreanStock)
            
            if symbol:
                query = query.filter(KoreanStock.symbol == symbol)
            
            if market:
                query = query.filter(KoreanStock.market == market)
            
            if start_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                query = query.filter(KoreanStock.date >= start_dt)
            
            if end_date:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                query = query.filter(KoreanStock.date <= end_dt)
            
            results = query.all()
            
            # Convert to DataFrame
            data = []
            for record in results:
                data.append({
                    'symbol': record.symbol,
                    'market': record.market,
                    'longname': record.longname,
                    'high': record.high,
                    'low': record.low,
                    'open_price': record.open_price,
                    'close': record.close,
                    'volume': record.volume,
                    'dividends': record.dividends,
                    'stock_splits': record.stock_splits,
                    'date': record.date
                })
            
            session.close()
            
            df = pd.DataFrame(data)
            self.logger.info(f"Retrieved {len(df)} records from database")
            return df
            
        except Exception as e:
            self.logger.error(f"Error retrieving data from database: {e}")
            session.close()
            return pd.DataFrame()
    
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


# Convenience functions
def collect_all_korean_stocks_data(database_url: str = None) -> pd.DataFrame:
    """Collect data for all Korean stocks and save to database."""
    collector = KoreanStockCollector(database_url)
    return collector.collect_all_stocks_data()

def get_stock_data_from_db(symbol: str = None, market: str = None, 
                          start_date: str = None, end_date: str = None,
                          database_url: str = None) -> pd.DataFrame:
    """Get stock data from database."""
    collector = KoreanStockCollector(database_url)
    return collector.get_stock_data_from_db(symbol, market, start_date, end_date)

def get_all_stock_codes_from_pykrx() -> List[Dict[str, Any]]:
    """Get all stock codes from pykrx."""
    collector = KoreanStockCollector()
    return collector.get_all_stock_codes_from_pykrx() 