#!/usr/bin/env python3
"""
Historical Market Data Collection Script

This script collects historical OHLCV data for KOSPI and KOSDAQ markets
from 1990-01-01 to today using pykrx. This is a one-time use script
to build a comprehensive historical database.

Usage:
    python historical_market_data.py

Features:
    - Collects daily OHLCV data for both KOSPI and KOSDAQ
    - Implements rate limiting to prevent API overload
    - Saves data to SQLite database table
    - Provides progress tracking and error handling
    - Resumes from last successful date if interrupted
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
import warnings
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

# Suppress pkg_resources deprecation warning
warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated",
    category=UserWarning,
    module="pykrx"
)

try:
    from pykrx import stock
    from utils.logger import get_logger
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please ensure pykrx and other dependencies are installed.")
    sys.exit(1)

# Database setup
Base = declarative_base()

class KRStockMarketPrice(Base):
    """Database model for Korean stock market price data."""
    __tablename__ = 'kr_stock_market_price'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    day_of_week = Column(String(10), nullable=False)
    stock_symbol = Column(String(10), nullable=False, index=True)
    market_name = Column(String(10), nullable=False, index=True)
    start_price = Column(Integer, nullable=True)
    day_high_price = Column(Integer, nullable=True)
    day_low_price = Column(Integer, nullable=True)
    day_end_price = Column(Integer, nullable=True)
    transaction_volume = Column(Integer, nullable=True)
    transaction_value = Column(Integer, nullable=True)
    delta = Column(Float, nullable=True)
    market_cap = Column(Integer, nullable=True)
    created_at = Column(Date, default=datetime.now().date)
    
    # Composite index for efficient queries
    __table_args__ = (
        {'sqlite_autoincrement': True}
    )


class HistoricalMarketDataCollector:
    """
    Collects historical market OHLCV data for Korean markets.
    """
    
    def __init__(self, 
                 start_date: str = "1990-01-01",
                 database_url: str = "data/raw/korean_stocks/kr_stock_codes.db",
                 sleep_interval: float = 1.0):
        """
        Initialize the historical market data collector.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            database_url: SQLite database file path
            sleep_interval: Sleep time between API calls (seconds)
        """
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.now()
        self.database_url = f"sqlite:///{database_url}"
        self.sleep_interval = sleep_interval
        
        # Setup database
        self.engine = create_engine(self.database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create table if it doesn't exist
        Base.metadata.create_all(bind=self.engine)
        
        # Setup logging
        self.logger = get_logger("historical_market_data")
        
        # Markets to collect
        self.markets = ["KOSPI", "KOSDAQ"]
        
        # Progress tracking
        self.progress_file = Path(database_url).parent / "collection_progress.txt"
        self.stats = {
            "total_days": 0,
            "processed_days": 0,
            "successful_days": 0,
            "failed_days": 0,
            "start_time": None,
            "last_successful_date": None,
            "total_records": 0
        }
        
        self.logger.info(f"Initialized HistoricalMarketDataCollector")
        self.logger.info(f"Date range: {self.start_date.date()} to {self.end_date.date()}")
        self.logger.info(f"Database: {database_url}")
        self.logger.info(f"Sleep interval: {self.sleep_interval} seconds")
    
    def get_date_range(self) -> List[datetime]:
        """
        Generate list of dates from start_date to end_date.
        
        Returns:
            List of datetime objects for each day
        """
        dates = []
        current_date = self.start_date
        
        while current_date <= self.end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        
        return dates
    
    def format_date_for_pykrx(self, date: datetime) -> str:
        """
        Format date for pykrx API (YYYYMMDD format).
        
        Args:
            date: datetime object
            
        Returns:
            Date string in YYYYMMDD format
        """
        return date.strftime("%Y%m%d")
    
    def get_day_of_week(self, date: datetime) -> str:
        """
        Get day of week in English.
        
        Args:
            date: datetime object
            
        Returns:
            Day of week string (Monday, Tuesday, etc.)
        """
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return days[date.weekday()]
    
    def transform_data_for_db(self, df: pd.DataFrame, date: datetime, market: str) -> List[KRStockMarketPrice]:
        """
        Transform DataFrame to database model objects.
        
        Args:
            df: DataFrame from pykrx
            date: Date of the data
            market: Market name (KOSPI or KOSDAQ)
            
        Returns:
            List of KRStockMarketPrice objects
        """
        records = []
        day_of_week = self.get_day_of_week(date)
        
        for ticker, row in df.iterrows():
            record = KRStockMarketPrice(
                date=date.date(),
                day_of_week=day_of_week,
                stock_symbol=str(ticker),
                market_name=market,
                start_price=row.get('시가', None),
                day_high_price=row.get('고가', None),
                day_low_price=row.get('저가', None),
                day_end_price=row.get('종가', None),
                transaction_volume=row.get('거래량', None),
                transaction_value=row.get('거래대금', None),
                delta=row.get('등락률', None),
                market_cap=row.get('시가총액', None)
            )
            records.append(record)
        
        return records
    
    def save_market_data_to_db(self, date: datetime, market: str, data: pd.DataFrame) -> bool:
        """
        Save market data to database.
        
        Args:
            date: Date of the data
            market: Market name (KOSPI or KOSDAQ)
            data: DataFrame containing OHLCV data
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            session = self.SessionLocal()
            
            # Check if data already exists for this date and market
            existing_count = session.query(KRStockMarketPrice).filter(
                KRStockMarketPrice.date == date.date(),
                KRStockMarketPrice.market_name == market
            ).count()
            
            if existing_count > 0:
                # Delete existing data for this date and market
                session.query(KRStockMarketPrice).filter(
                    KRStockMarketPrice.date == date.date(),
                    KRStockMarketPrice.market_name == market
                ).delete()
                self.logger.debug(f"Deleted {existing_count} existing records for {market} on {date.date()}")
            
            # Transform data
            records = self.transform_data_for_db(data, date, market)
            
            # Add records to session
            session.add_all(records)
            
            # Commit transaction
            session.commit()
            session.close()
            
            self.logger.debug(f"Saved {len(records)} {market} records for {date.date()}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving {market} data for {date.date()}: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def collect_market_data_for_date(self, date: datetime) -> Dict[str, bool]:
        """
        Collect market data for a specific date for both markets.
        
        Args:
            date: Date to collect data for
            
        Returns:
            Dictionary with market names as keys and success status as values
        """
        date_str = self.format_date_for_pykrx(date)
        results = {}
        
        for market in self.markets:
            try:
                self.logger.info(f"Collecting {market} data for {date.date()} ({date_str})")
                
                # Get market data
                df = stock.get_market_ohlcv(date_str, market=market)
                
                if df is not None and not df.empty:
                    # Save data to database
                    success = self.save_market_data_to_db(date, market, df)
                    results[market] = success
                    
                    if success:
                        self.logger.info(f"✅ {market}: {len(df)} stocks collected for {date.date()}")
                        self.stats["total_records"] += len(df)
                    else:
                        self.logger.error(f"❌ {market}: Failed to save data for {date.date()}")
                else:
                    self.logger.warning(f"⚠️ {market}: No data available for {date.date()}")
                    results[market] = False
                
            except Exception as e:
                # Handle errors gracefully - treat as "no data available"
                self.logger.warning(f"⚠️ {market}: No data available for {date.date()} (Error: {str(e)[:100]}...)")
                results[market] = False
                
                # Continue with next market instead of stopping
                continue
            
            # Sleep between markets
            time.sleep(self.sleep_interval / 2)
        
        return results
    
    def update_progress(self, date: datetime, results: Dict[str, bool]):
        """
        Update progress tracking.
        
        Args:
            date: Date that was processed
            results: Results from data collection
        """
        self.stats["processed_days"] += 1
        
        # Check if at least one market was successful
        if any(results.values()):
            self.stats["successful_days"] += 1
            self.stats["last_successful_date"] = date.strftime("%Y-%m-%d")
        else:
            self.stats["failed_days"] += 1
        
        # Save progress to file
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                f.write(f"Last processed date: {date.strftime('%Y-%m-%d')}\n")
                f.write(f"Last successful date: {self.stats['last_successful_date']}\n")
                f.write(f"Total days: {self.stats['total_days']}\n")
                f.write(f"Processed days: {self.stats['processed_days']}\n")
                f.write(f"Successful days: {self.stats['successful_days']}\n")
                f.write(f"Failed days: {self.stats['failed_days']}\n")
                f.write(f"Total records: {self.stats['total_records']}\n")
                f.write(f"Success rate: {(self.stats['successful_days']/self.stats['processed_days']*100):.2f}%\n")
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
    
    def load_progress(self) -> Optional[datetime]:
        """
        Load progress from file to resume from last successful date.
        
        Returns:
            Last successful date or None if no progress file
        """
        if not self.progress_file.exists():
            return None
        
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    if line.startswith("Last successful date:"):
                        date_str = line.split(": ")[1].strip()
                        return datetime.strptime(date_str, "%Y-%m-%d")
        except Exception as e:
            self.logger.error(f"Error loading progress: {e}")
        
        return None
    
    def get_database_stats(self):
        """Get current database statistics."""
        try:
            session = self.SessionLocal()
            
            # Total records
            total_records = session.query(KRStockMarketPrice).count()
            
            # Records by market
            kospi_records = session.query(KRStockMarketPrice).filter(
                KRStockMarketPrice.market_name == "KOSPI"
            ).count()
            
            kosdaq_records = session.query(KRStockMarketPrice).filter(
                KRStockMarketPrice.market_name == "KOSDAQ"
            ).count()
            
            # Date range
            min_date = session.query(KRStockMarketPrice.date).order_by(KRStockMarketPrice.date).first()
            max_date = session.query(KRStockMarketPrice.date).order_by(KRStockMarketPrice.date.desc()).first()
            
            session.close()
            
            return {
                "total_records": total_records,
                "kospi_records": kospi_records,
                "kosdaq_records": kosdaq_records,
                "min_date": min_date[0] if min_date else None,
                "max_date": max_date[0] if max_date else None
            }
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def print_stats(self):
        """Print current collection statistics."""
        if self.stats["processed_days"] > 0:
            success_rate = (self.stats["successful_days"] / self.stats["processed_days"]) * 100
            elapsed_time = datetime.now() - self.stats["start_time"]
            
            # Get database stats
            db_stats = self.get_database_stats()
            
            print("\n" + "="*60)
            print("📊 COLLECTION STATISTICS")
            print("="*60)
            print(f"Total days to process: {self.stats['total_days']}")
            print(f"Days processed: {self.stats['processed_days']}")
            print(f"Successful days: {self.stats['successful_days']}")
            print(f"Failed days: {self.stats['failed_days']}")
            print(f"Success rate: {success_rate:.2f}%")
            print(f"Elapsed time: {elapsed_time}")
            print(f"Last successful date: {self.stats['last_successful_date']}")
            print(f"Records collected this session: {self.stats['total_records']}")
            
            if db_stats:
                print(f"\n📈 DATABASE STATISTICS")
                print(f"Total records in database: {db_stats['total_records']:,}")
                print(f"KOSPI records: {db_stats['kospi_records']:,}")
                print(f"KOSDAQ records: {db_stats['kosdaq_records']:,}")
                if db_stats['min_date'] and db_stats['max_date']:
                    print(f"Date range: {db_stats['min_date']} to {db_stats['max_date']}")
            
            if self.stats["processed_days"] < self.stats["total_days"]:
                remaining_days = self.stats["total_days"] - self.stats["processed_days"]
                estimated_time = (elapsed_time / self.stats["processed_days"]) * remaining_days
                print(f"\nRemaining days: {remaining_days}")
                print(f"Estimated time remaining: {estimated_time}")
            print("="*60)
    
    def run(self, resume: bool = True):
        """
        Run the historical data collection.
        
        Args:
            resume: Whether to resume from last successful date
        """
        self.stats["start_time"] = datetime.now()
        
        # Get date range
        all_dates = self.get_date_range()
        self.stats["total_days"] = len(all_dates)
        
        # Check for resume
        if resume:
            last_successful = self.load_progress()
            if last_successful:
                # Find the next date after last successful
                try:
                    start_index = all_dates.index(last_successful) + 1
                    all_dates = all_dates[start_index:]
                    self.logger.info(f"Resuming from {last_successful.date()}")
                except ValueError:
                    self.logger.warning("Could not find last successful date, starting from beginning")
        
        # Check if there are any dates to process
        if not all_dates:
            self.logger.info("No dates to process - collection may already be complete!")
            self.print_stats()
            return True
        
        self.logger.info(f"Starting collection of {len(all_dates)} days of market data")
        self.logger.info(f"First date: {all_dates[0].date()}")
        self.logger.info(f"Last date: {all_dates[-1].date()}")
        
        try:
            for i, date in enumerate(all_dates):
                # Print progress every 10 days
                if i % 10 == 0:
                    self.print_stats()
                
                # Collect data for this date
                results = self.collect_market_data_for_date(date)
                
                # Update progress
                self.update_progress(date, results)
                
                # Sleep between dates
                time.sleep(self.sleep_interval)
                
                # Print progress every 100 days
                if (i + 1) % 100 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(all_dates)} days")
        
        except KeyboardInterrupt:
            self.logger.info("Collection interrupted by user")
            self.print_stats()
            return False
        
        except Exception as e:
            self.logger.error(f"Unexpected error during collection: {e}")
            self.print_stats()
            return False
        
        self.logger.info("Collection completed successfully!")
        self.print_stats()
        return True


def main():
    """Main function to run the historical market data collection."""
    print("🚀 Historical Market Data Collection")
    print("="*50)
    
    # Configuration
    START_DATE = "1990-01-01"
    DATABASE_URL = "data/raw/korean_stocks/kr_stock_codes.db"
    SLEEP_INTERVAL = 1.0  # 1 second between API calls
    
    # Create collector
    collector = HistoricalMarketDataCollector(
        start_date=START_DATE,
        database_url=DATABASE_URL,
        sleep_interval=SLEEP_INTERVAL
    )
    
    # Ask user for confirmation
    print(f"\nConfiguration:")
    print(f"  Start date: {START_DATE}")
    print(f"  End date: {datetime.now().date()}")
    print(f"  Database: {DATABASE_URL}")
    print(f"  Table: kr_stock_market_price")
    print(f"  Sleep interval: {SLEEP_INTERVAL} seconds")
    print(f"  Estimated total days: ~{collector.stats['total_days']}")
    print(f"  Estimated time: ~{collector.stats['total_days'] * SLEEP_INTERVAL / 3600:.1f} hours")
    
    response = input("\nDo you want to proceed? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("Collection cancelled.")
        return
    
    # Run collection
    success = collector.run(resume=True)
    
    if success:
        print("\n✅ Historical market data collection completed successfully!")
    else:
        print("\n❌ Historical market data collection failed or was interrupted.")
        print("You can resume by running the script again.")


if __name__ == "__main__":
    main()
