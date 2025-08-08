"""
Korean Stock Codes Collection and Management Script.
This module creates and maintains a table of all Korean stock codes with IPO and delisting dates.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
from pathlib import Path

# Database imports
from sqlalchemy import create_engine, Column, String, Date, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# pykrx for Korean stock data
from pykrx import stock

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Database setup
Base = declarative_base()

class KRStockCode(Base):
    """Database table for Korean stock codes."""
    __tablename__ = 'kr_stock_codes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(Integer, nullable=False, unique=True, index=True)
    stock_market = Column(String(10), nullable=False)  # KOSPI or KOSDAQ
    ipo_date = Column(Date, nullable=True)
    delisting_date = Column(Date, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(Date, default=datetime.now().date)
    updated_at = Column(Date, default=datetime.now().date)


class KRStockCodeCollector:
    """
    Collects and maintains Korean stock codes with IPO and delisting information.
    """
    
    def __init__(self, database_url: str = None):
        """Initialize the Korean stock code collector."""
        self.logger = logger
        self.logger.info("KR Stock Code Collector initialized")
        
        # Database setup
        if database_url:
            self.database_url = database_url
        else:
            # Default SQLite connection
            self.database_url = "sqlite:///./kr_stock_codes.db"
        
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
    
    def get_all_stock_codes_from_pykrx(self) -> List[Dict[str, Any]]:
        """
        Get all stock codes from pykrx with basic information.
        
        Returns:
            List of dictionaries containing stock information
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
                    
                    # Handle tickers with letters (like 00104K, 37550L, etc.)
                    # These are typically bond codes or special instruments, not regular stocks
                    if any(char.isalpha() for char in ticker):
                        continue  # Skip non-numeric tickers
                    
                    company_info = {
                        'stock_code': int(ticker),
                        'stock_market': market,
                        'company_name': name
                    }
                    companies.append(company_info)
                    
                except Exception as e:
                    self.logger.warning(f"Error getting info for ticker {ticker}: {e}")
                    continue
            
            self.logger.info(f"Retrieved {len(companies)} companies from pykrx")
            return companies
            
        except Exception as e:
            self.logger.error(f"Error fetching data from pykrx: {e}")
            return []
    
    def get_ipo_date_from_pykrx(self, stock_code: str) -> Optional[datetime]:
        """
        Get IPO date for a specific stock code from pykrx.
        
        Args:
            stock_code: Stock code as string
            
        Returns:
            IPO date as datetime object or None if not found
        """
        try:
            # Get stock information from pykrx
            # Note: pykrx doesn't directly provide IPO dates, so we'll use a fallback approach
            # We'll get the earliest available data date as a proxy for IPO date
            
            # Get historical data to find the earliest date
            df = stock.get_market_ohlcv_by_date(
                fromdate="1990-01-01", 
                todate=datetime.now().strftime("%Y-%m-%d"), 
                ticker=stock_code
            )
            
            if not df.empty:
                # Get the earliest date as proxy for IPO date
                earliest_date = df.index.min()
                return earliest_date.date()
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting IPO date for {stock_code}: {e}")
            return None
    
    def check_if_delisted(self, stock_code: str) -> Optional[datetime]:
        """
        Check if a stock has been delisted by verifying if the stock code stops appearing after a certain date.
        The delisting date is assumed to be the last date the code appears in pykrx data.

        Args:
            stock_code: Stock code as string

        Returns:
            Delisting date if delisted (last available trading date), None if still active
        """
        try:
            # Define the period to check for recent activity (last 30 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            # Get recent OHLCV data for the stock
            df_recent = stock.get_market_ohlcv_by_date(
                fromdate=start_date.strftime("%Y-%m-%d"),
                todate=end_date.strftime("%Y-%m-%d"),
                ticker=stock_code
            )

            # If there is recent data, the stock is still active
            if not df_recent.empty:
                return None  # Stock is still active

            # If no recent data, check the last date the stock code appeared in the market
            # We'll look back from a reasonable start date (e.g., 1990-01-01)
            df_all = stock.get_market_ohlcv_by_date(
                fromdate="1990-01-01",
                todate=end_date.strftime("%Y-%m-%d"),
                ticker=stock_code
            )

            if not df_all.empty:
                # The last date the stock code appears is assumed to be the delisting date
                last_date = df_all.index.max()
                return last_date.date()
            else:
                # If the stock never appears, we cannot determine delisting date
                return None

        except Exception as e:
            self.logger.warning(f"Error checking delisting status for {stock_code}: {e}")
            return None
    def collect_and_update_stock_codes(self) -> bool:
        """
        Collect all stock codes and update the database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Starting stock code collection and update")
            
            # Get all stock codes from pykrx
            companies = self.get_all_stock_codes_from_pykrx()
            
            if not companies:
                self.logger.error("No companies found from pykrx")
                return False
            
            session = self.SessionLocal()
            
            # Process each company
            for i, company in enumerate(companies):
                try:
                    stock_code = company['stock_code']
                    stock_market = company['stock_market']
                    
                    self.logger.info(f"Processing {i+1}/{len(companies)}: {stock_code} ({stock_market})")
                    
                    # Check if record already exists
                    existing_record = session.query(KRStockCode).filter(
                        KRStockCode.stock_code == stock_code
                    ).first()
                    
                    if existing_record:
                        # Update existing record
                        existing_record.stock_market = stock_market
                        existing_record.updated_at = datetime.now().date()
                        
                        # Check delisting status
                        delisting_date = self.check_if_delisted(str(stock_code))
                        if delisting_date:
                            existing_record.delisting_date = delisting_date
                            existing_record.is_active = False
                        else:
                            existing_record.delisting_date = None
                            existing_record.is_active = True
                            
                    else:
                        # Create new record
                        # Get IPO date
                        ipo_date = self.get_ipo_date_from_pykrx(str(stock_code))
                        
                        # Check delisting status
                        delisting_date = self.check_if_delisted(str(stock_code))
                        is_active = delisting_date is None
                        
                        new_record = KRStockCode(
                            stock_code=stock_code,
                            stock_market=stock_market,
                            ipo_date=ipo_date if ipo_date else None,
                            delisting_date=delisting_date if delisting_date else None,
                            is_active=is_active,
                            created_at=datetime.now().date(),
                            updated_at=datetime.now().date()
                        )
                        session.add(new_record)
                    
                    # Commit every 100 records to avoid long transactions
                    if (i + 1) % 100 == 0:
                        session.commit()
                        self.logger.info(f"Committed {i + 1} records")
                    
                except Exception as e:
                    self.logger.error(f"Error processing stock code {company.get('stock_code', 'Unknown')}: {e}")
                    continue
            
            # Final commit
            session.commit()
            session.close()
            
            self.logger.info("Stock code collection and update completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in stock code collection: {e}")
            session.rollback()
            session.close()
            return False
    
    def get_stock_codes_from_db(self, market: str = None, active_only: bool = True) -> pd.DataFrame:
        """
        Retrieve stock codes from database.
        
        Args:
            market: Filter by market (KOSPI or KOSDAQ)
            active_only: If True, only return active (non-delisted) stocks
            
        Returns:
            DataFrame containing stock codes
        """
        try:
            session = self.SessionLocal()
            query = session.query(KRStockCode)
            
            if market:
                query = query.filter(KRStockCode.stock_market == market)
            
            if active_only:
                query = query.filter(KRStockCode.is_active == True)
            
            results = query.all()
            
            # Convert to DataFrame
            data = []
            for record in results:
                data.append({
                    'stock_code': record.stock_code,
                    'stock_market': record.stock_market,
                    'ipo_date': record.ipo_date,
                    'delisting_date': record.delisting_date,
                    'is_active': record.is_active,
                    'created_at': record.created_at,
                    'updated_at': record.updated_at
                })
            
            session.close()
            
            df = pd.DataFrame(data)
            self.logger.info(f"Retrieved {len(df)} stock codes from database")
            return df
            
        except Exception as e:
            self.logger.error(f"Error retrieving stock codes from database: {e}")
            session.close()
            return pd.DataFrame()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the stock codes in the database.
        
        Returns:
            Dictionary containing statistics
        """
        try:
            session = self.SessionLocal()
            
            # Total counts
            total_count = session.query(KRStockCode).count()
            kospi_count = session.query(KRStockCode).filter(KRStockCode.stock_market == 'KOSPI').count()
            kosdaq_count = session.query(KRStockCode).filter(KRStockCode.stock_market == 'KOSDAQ').count()
            active_count = session.query(KRStockCode).filter(KRStockCode.is_active == True).count()
            delisted_count = session.query(KRStockCode).filter(KRStockCode.is_active == False).count()
            
            # IPO date statistics
            ipo_dates = session.query(KRStockCode.ipo_date).filter(KRStockCode.ipo_date.isnot(None)).all()
            ipo_dates = [date[0] for date in ipo_dates if date[0]]
            
            stats = {
                'total_stocks': total_count,
                'kospi_stocks': kospi_count,
                'kosdaq_stocks': kosdaq_count,
                'active_stocks': active_count,
                'delisted_stocks': delisted_count,
                'with_ipo_date': len(ipo_dates),
                'earliest_ipo': min(ipo_dates) if ipo_dates else None,
                'latest_ipo': max(ipo_dates) if ipo_dates else None
            }
            
            session.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}


# Convenience functions
def collect_kr_stock_codes(database_url: str = None) -> bool:
    """Collect and update Korean stock codes."""
    collector = KRStockCodeCollector(database_url)
    return collector.collect_and_update_stock_codes()

def get_kr_stock_codes(market: str = None, active_only: bool = True, database_url: str = None) -> pd.DataFrame:
    """Get Korean stock codes from database."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_stock_codes_from_db(market, active_only)

def get_kr_stock_statistics(database_url: str = None) -> Dict[str, Any]:
    """Get statistics about Korean stock codes."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_statistics()
