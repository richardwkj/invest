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

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import get_logger

logger = get_logger(__name__)

# Database setup
Base = declarative_base()

class KRStockCode(Base):
    """Database table for Korean stock codes."""
    __tablename__ = 'kr_stock_codes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False, unique=True, index=True)  # Changed to String to accommodate non-numeric tickers
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
            # Default SQLite connection - save to data/raw/korean_stocks directory
            data_dir = Path("data/raw/korean_stocks")
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = data_dir / "kr_stock_codes.db"
            self.database_url = f"sqlite:///{db_path}"
        
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
    
    def cleanup_duplicates(self) -> int:
        """
        Remove duplicate stock codes from the database, keeping the most recent record.
        
        Returns:
            Number of duplicate records removed
        """
        try:
            session = self.SessionLocal()
            
            # Find duplicates based on stock_code
            duplicates = session.query(KRStockCode.stock_code).group_by(
                KRStockCode.stock_code
            ).having(
                session.query(KRStockCode).filter(
                    KRStockCode.stock_code == KRStockCode.stock_code
                ).count() > 1
            ).all()
            
            removed_count = 0
            
            for (stock_code,) in duplicates:
                # Get all records for this stock code
                records = session.query(KRStockCode).filter(
                    KRStockCode.stock_code == stock_code
                ).order_by(KRStockCode.updated_at.desc()).all()
                
                # Keep the first (most recent) record, delete the rest
                for record in records[1:]:
                    session.delete(record)
                    removed_count += 1
            
            session.commit()
            session.close()
            
            if removed_count > 0:
                self.logger.info(f"Removed {removed_count} duplicate records")
            else:
                self.logger.info("No duplicate records found")
                
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up duplicates: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return 0
    
    def get_all_stock_codes_from_pykrx(self) -> List[Dict[str, Any]]:
        """
        Get all stock codes from pykrx with basic information.
        
        Returns:
            List of dictionaries containing stock information
        """
        try:
            self.logger.info("Fetching all stock codes from pykrx")
            
            companies = []
            
            # Get KOSPI tickers
            self.logger.info("Fetching KOSPI tickers...")
            kospi_tickers = stock.get_market_ticker_list(market="KOSPI")
            self.logger.info(f"Found {len(kospi_tickers)} KOSPI tickers")
            
            # Get KOSDAQ tickers
            self.logger.info("Fetching KOSDAQ tickers...")
            kosdaq_tickers = stock.get_market_ticker_list(market="KOSDAQ")
            self.logger.info(f"Found {len(kosdaq_tickers)} KOSDAQ tickers")
            
            # Process KOSPI tickers
            for ticker in kospi_tickers:
                try:
                    # Get stock name
                    name = stock.get_market_ticker_name(ticker)
                    
                    company_info = {
                        'stock_code': ticker,  # Keep as string to accommodate non-numeric tickers
                        'stock_market': 'KOSPI',
                        'company_name': name
                    }
                    companies.append(company_info)
                    
                except Exception as e:
                    self.logger.warning(f"Error getting info for KOSPI ticker {ticker}: {e}")
                    continue
            
            # Process KOSDAQ tickers
            for ticker in kosdaq_tickers:
                try:
                    # Get stock name
                    name = stock.get_market_ticker_name(ticker)
                    
                    company_info = {
                        'stock_code': ticker,  # Keep as string to accommodate non-numeric tickers
                        'stock_market': 'KOSDAQ',
                        'company_name': name
                    }
                    companies.append(company_info)
                    
                except Exception as e:
                    self.logger.warning(f"Error getting info for KOSDAQ ticker {ticker}: {e}")
                    continue
            
            self.logger.info(f"Retrieved {len(companies)} companies from pykrx (KOSPI + KOSDAQ)")
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
        This method ensures no duplicate rows are created by properly updating existing records.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Starting stock code collection and update")
            
            # First, cleanup any existing duplicates
            self.logger.info("Checking for existing duplicates...")
            removed_duplicates = self.cleanup_duplicates()
            
            # Get all stock codes from pykrx
            companies = self.get_all_stock_codes_from_pykrx()
            
            if not companies:
                self.logger.error("No companies found from pykrx")
                return False
            
            session = self.SessionLocal()
            
            # Track statistics
            updated_count = 0
            created_count = 0
            error_count = 0
            
            # Process each company
            for i, company in enumerate(companies):
                try:
                    stock_code = company['stock_code']
                    stock_market = company['stock_market']
                    
                    if (i + 1) % 100 == 0:
                        self.logger.info(f"Processing {i+1}/{len(companies)}: {stock_code} ({stock_market})")
                    
                    # Check if record already exists
                    existing_record = session.query(KRStockCode).filter(
                        KRStockCode.stock_code == stock_code
                    ).first()
                    
                    if existing_record:
                        # Update existing record - only update fields that might have changed
                        updated = False
                        
                        # Update market if different
                        if existing_record.stock_market != stock_market:
                            existing_record.stock_market = stock_market
                            updated = True
                        
                        # Check delisting status
                        delisting_date = self.check_if_delisted(str(stock_code))
                        if delisting_date != existing_record.delisting_date:
                            existing_record.delisting_date = delisting_date
                            existing_record.is_active = False
                            updated = True
                        elif delisting_date is None and not existing_record.is_active:
                            # Stock is active again (was delisted but now has recent data)
                            existing_record.delisting_date = None
                            existing_record.is_active = True
                            updated = True
                        
                        # Update timestamp if any changes were made
                        if updated:
                            existing_record.updated_at = datetime.now().date()
                            updated_count += 1
                            
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
                        created_count += 1
                    
                    # Commit every 100 records to avoid long transactions
                    if (i + 1) % 100 == 0:
                        session.commit()
                        self.logger.info(f"Committed {i + 1} records (Created: {created_count}, Updated: {updated_count}, Errors: {error_count})")
                    
                except Exception as e:
                    self.logger.error(f"Error processing stock code {company.get('stock_code', 'Unknown')}: {e}")
                    error_count += 1
                    continue
            
            # Final commit
            session.commit()
            session.close()
            
            self.logger.info(f"Stock code collection and update completed successfully")
            self.logger.info(f"Final statistics - Created: {created_count}, Updated: {updated_count}, Errors: {error_count}, Total processed: {len(companies)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in stock code collection: {e}")
            if 'session' in locals():
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
    
    def save_to_csv(self, market: str = None, active_only: bool = True, filename: str = None) -> str:
        """
        Save stock codes data to CSV file in the data/raw/korean_stocks directory.
        
        Args:
            market: Filter by market (KOSPI or KOSDAQ)
            active_only: If True, only save active (non-delisted) stocks
            filename: Custom filename (optional)
            
        Returns:
            Path to the saved CSV file
        """
        try:
            # Get data from database
            df = self.get_stock_codes_from_db(market, active_only)
            
            if df.empty:
                self.logger.warning("No data to save to CSV")
                return ""
            
            # Create data directory if it doesn't exist
            data_dir = Path("data/raw/korean_stocks")
            data_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename if not provided
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                market_suffix = f"_{market}" if market else ""
                active_suffix = "_active" if active_only else "_all"
                filename = f"kr_stock_codes{market_suffix}{active_suffix}_{timestamp}.csv"
            
            # Save to CSV
            csv_path = data_dir / filename
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"Data saved to CSV: {csv_path}")
            return str(csv_path)
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            return ""
    
    def save_all_formats(self) -> Dict[str, str]:
        """
        Save stock codes data in multiple formats to data/raw/korean_stocks directory.
        
        Returns:
            Dictionary with paths to saved files
        """
        try:
            saved_files = {}
            
            # Save all stocks (active + delisted)
            all_stocks_path = self.save_to_csv(active_only=False, filename="kr_stock_codes_all.csv")
            if all_stocks_path:
                saved_files['all_stocks'] = all_stocks_path
            
            # Save active stocks only
            active_stocks_path = self.save_to_csv(active_only=True, filename="kr_stock_codes_active.csv")
            if active_stocks_path:
                saved_files['active_stocks'] = active_stocks_path
            
            # Save KOSPI stocks
            kospi_path = self.save_to_csv(market="KOSPI", filename="kr_stock_codes_kospi.csv")
            if kospi_path:
                saved_files['kospi_stocks'] = kospi_path
            
            # Save KOSDAQ stocks
            kosdaq_path = self.save_to_csv(market="KOSDAQ", filename="kr_stock_codes_kosdaq.csv")
            if kosdaq_path:
                saved_files['kosdaq_stocks'] = kosdaq_path
            
            # Save statistics
            stats = self.get_statistics()
            if stats:
                stats_df = pd.DataFrame([stats])
                data_dir = Path("data/raw/korean_stocks")
                stats_path = data_dir / "kr_stock_codes_statistics.csv"
                stats_df.to_csv(stats_path, index=False, encoding='utf-8-sig')
                saved_files['statistics'] = str(stats_path)
            
            self.logger.info(f"All data saved to: {data_dir}")
            return saved_files
            
        except Exception as e:
            self.logger.error(f"Error saving all formats: {e}")
            return {}


# Convenience functions
def collect_kr_stock_codes(database_url: str = None) -> bool:
    """Collect and update Korean stock codes."""
    collector = KRStockCodeCollector(database_url)
    return collector.collect_and_update_stock_codes()

def cleanup_kr_stock_codes_duplicates(database_url: str = None) -> int:
    """Clean up duplicate Korean stock codes from database."""
    collector = KRStockCodeCollector(database_url)
    return collector.cleanup_duplicates()

def get_kr_stock_codes(market: str = None, active_only: bool = True, database_url: str = None) -> pd.DataFrame:
    """Get Korean stock codes from database."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_stock_codes_from_db(market, active_only)

def get_kr_stock_statistics(database_url: str = None) -> Dict[str, Any]:
    """Get statistics about Korean stock codes."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_statistics()

def save_kr_stock_codes_to_csv(market: str = None, active_only: bool = True, 
                              filename: str = None, database_url: str = None) -> str:
    """Save Korean stock codes to CSV file."""
    collector = KRStockCodeCollector(database_url)
    return collector.save_to_csv(market, active_only, filename)

def save_kr_stock_codes_all_formats(database_url: str = None) -> Dict[str, str]:
    """Save Korean stock codes in all formats (CSV files)."""
    collector = KRStockCodeCollector(database_url)
    return collector.save_all_formats()
