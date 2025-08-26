"""
Korean Stock Symbols Collection and Management Script.
This module creates and maintains a table of all Korean stock symbols with company names, IPO and delisting dates.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
from pathlib import Path

# Database imports
from sqlalchemy import create_engine, Column, String, Date, Integer, Boolean, text
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
    stock_symbol = Column(String(10), nullable=False, unique=True, index=True)  # Renamed from stock_code to stock_symbol
    stock_market = Column(String(10), nullable=False)  # KOSPI or KOSDAQ
    company_name = Column(String(200), nullable=True)  # New column for company names
    ipo_date = Column(Date, nullable=True)
    delisting_date = Column(Date, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(Date, default=datetime.now().date)
    updated_at = Column(Date, default=datetime.now().date)


class KRStockCodeCollector:
    """
    Collects and maintains Korean stock symbols with company names, IPO and delisting information.
    """
    
    def __init__(self, database_url: str = None):
        """Initialize the Korean stock symbol collector."""
        self.logger = logger
        self.logger.info("KR Stock Symbol Collector initialized")
        
        # Database setup
        if database_url:
            self.database_url = database_url
        else:
            # Default PostgreSQL connection
            self.database_url = "postgresql://username:password@localhost:5432/kr_stock_symbols"
            self.logger.warning("Using default PostgreSQL connection. Set database_url parameter for custom connection.")
        
        # Create PostgreSQL engine with connection pooling
        self.engine = create_engine(
            self.database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False  # Set to True for SQL debugging
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables
        self._create_tables()
        
    def _create_tables(self):
        """Create database tables if they don't exist."""
        try:
            # Create database if it doesn't exist
            self.create_database_if_not_exists()
            
            # Test database connection
            self._test_connection()
            
            # Create tables
            Base.metadata.create_all(bind=self.engine)
            
            # Create additional indexes for database performance
            self._create_database_indexes()
            
            self.logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating database tables: {e}")
            raise
    
    def _test_connection(self):
        """Test database connection."""
        try:
            with self.engine.connect() as connection:
                if 'sqlite' in self.database_url.lower():
                    # SQLite connection test
                    result = connection.execute(text("SELECT sqlite_version();"))
                    version = result.fetchone()[0]
                    self.logger.info(f"SQLite connection successful. Version: {version}")
                else:
                    # PostgreSQL connection test
                    result = connection.execute(text("SELECT version();"))
                    version = result.fetchone()[0]
                    self.logger.info(f"PostgreSQL connection successful. Version: {version}")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            if 'sqlite' in self.database_url.lower():
                self.logger.error("SQLite connection failed. Check file permissions and path.")
            else:
                self.logger.error("PostgreSQL connection failed. Please ensure PostgreSQL is running and accessible")
                self.logger.error("Check your database_url or set up PostgreSQL with:")
                self.logger.error("  - Database: kr_stock_symbols")
                self.logger.error("  - User: username (with appropriate permissions)")
                self.logger.error("  - Password: password")
            raise
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database connection information."""
        try:
            with self.engine.connect() as connection:
                if 'sqlite' in self.database_url.lower():
                    # SQLite database info
                    result = connection.execute(text("SELECT sqlite_version();"))
                    version = result.fetchone()[0]
                    
                    # Get database file path
                    db_path = self.database_url.replace('sqlite:///', '')
                    
                    return {
                        'database_type': 'SQLite',
                        'database_path': db_path,
                        'server_version': version,
                        'connection_url': self.database_url
                    }
                else:
                    # PostgreSQL database info
                    # Get database name
                    result = connection.execute(text("SELECT current_database();"))
                    db_name = result.fetchone()[0]
                    
                    # Get current user
                    result = connection.execute(text("SELECT current_user;"))
                    current_user = result.fetchone()[0]
                    
                    # Get server version
                    result = connection.execute(text("SELECT version();"))
                    version = result.fetchone()[0]
                    
                    # Get connection count
                    result = connection.execute(text("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();"))
                    active_connections = result.fetchone()[0]
                    
                    return {
                        'database_type': 'PostgreSQL',
                        'database_name': db_name,
                        'current_user': current_user,
                        'server_version': version,
                        'active_connections': active_connections,
                        'connection_url': self.database_url.replace(self.database_url.split('@')[0].split('//')[1].split(':')[1], '***') if '@' in self.database_url else self.database_url
                    }
        except Exception as e:
            self.logger.error(f"Error getting database info: {e}")
            return {}
    
    def create_database_if_not_exists(self) -> bool:
        """Create the database if it doesn't exist."""
        try:
            if 'sqlite' in self.database_url.lower():
                # For SQLite, just ensure the directory exists
                from pathlib import Path
                db_path = Path(self.database_url.replace('sqlite:///', ''))
                db_path.parent.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"SQLite database directory ensured: {db_path.parent}")
                return True
            else:
                # PostgreSQL database creation
                # Parse the connection URL to get database name
                from urllib.parse import urlparse
                parsed = urlparse(self.database_url)
                db_name = parsed.path.lstrip('/')
                
                # Connect to default postgres database to create our database
                default_url = f"postgresql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}/postgres"
                default_engine = create_engine(default_url)
                
                with default_engine.connect() as connection:
                    # Check if database exists
                    result = connection.execute(text("SELECT 1 FROM pg_database WHERE datname = %s"), (db_name,))
                    exists = result.fetchone() is not None
                    
                    if not exists:
                        # Create database
                        connection.execute(text("COMMIT"))  # Close any open transaction
                        connection.execute(text(f"CREATE DATABASE {db_name}"))
                        self.logger.info(f"Database '{db_name}' created successfully")
                        return True
                    else:
                        self.logger.info(f"Database '{db_name}' already exists")
                        return True
                        
        except Exception as e:
            self.logger.error(f"Error creating database: {e}")
            return False
    
    def _create_database_indexes(self):
        """Create additional indexes for database performance."""
        try:
            with self.engine.connect() as connection:
                if 'sqlite' in self.database_url.lower():
                    # SQLite indexes
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_market_active 
                        ON kr_stock_codes(stock_market, is_active)
                    """))
                    
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_ipo_date 
                        ON kr_stock_codes(ipo_date)
                    """))
                    
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_delisting_date 
                        ON kr_stock_codes(delisting_date)
                    """))
                    
                    self.logger.info("SQLite indexes created successfully")
                    
                else:
                    # PostgreSQL indexes
                    # Create composite index for market and active status
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_market_active 
                        ON kr_stock_codes(stock_market, is_active)
                    """))
                    
                    # Create index for IPO dates
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_ipo_date 
                        ON kr_stock_codes(ipo_date)
                    """))
                    
                    # Create index for delisting dates
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_delisting_date 
                        ON kr_stock_codes(delisting_date)
                    """))
                    
                    # Create index for company names (for text search)
                    connection.execute(text("""
                        CREATE INDEX IF NOT EXISTS idx_kr_stock_codes_company_name 
                        ON kr_stock_codes(company_name)
                    """))
                    
                    self.logger.info("PostgreSQL indexes created successfully")
                
        except Exception as e:
            self.logger.warning(f"Error creating database indexes: {e}")
            # Don't fail if indexes can't be created
    
    def vacuum_and_analyze(self):
        """Run database maintenance (VACUUM ANALYZE for PostgreSQL, ANALYZE for SQLite)."""
        try:
            with self.engine.connect() as connection:
                if 'sqlite' in self.database_url.lower():
                    # SQLite maintenance
                    connection.execute(text("ANALYZE"))
                    self.logger.info("SQLite ANALYZE completed successfully")
                else:
                    # PostgreSQL maintenance
                    connection.execute(text("VACUUM ANALYZE kr_stock_codes"))
                    self.logger.info("PostgreSQL VACUUM ANALYZE completed successfully")
                
        except Exception as e:
            self.logger.warning(f"Error running database maintenance: {e}")
            # Don't fail if maintenance can't be run
    
    def cleanup_duplicates(self) -> int:
        """
        Remove duplicate stock symbols from the database, keeping the most recent record.
        
        Returns:
            Number of duplicate records removed
        """
        try:
            session = self.SessionLocal()
            
            # Find duplicates based on stock_symbol
            duplicates = session.query(KRStockCode.stock_symbol).group_by(
                KRStockCode.stock_symbol
            ).having(
                session.query(KRStockCode).filter(
                    KRStockCode.stock_symbol == KRStockCode.stock_symbol
                ).count() > 1
            ).all()
            
            removed_count = 0
            
            for (stock_symbol,) in duplicates:
                # Get all records for this stock symbol
                records = session.query(KRStockCode).filter(
                    KRStockCode.stock_symbol == stock_symbol
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
        Get all stock symbols from pykrx with company names and market information.
        
        Returns:
            List of dictionaries containing stock information
        """
        try:
            self.logger.info("Fetching all stock symbols and company names from pykrx")
            
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
            for i, ticker in enumerate(kospi_tickers):
                try:
                    # Get stock name from pykrx
                    name = stock.get_market_ticker_name(ticker)
                    
                    # Ensure company name is not empty
                    if not name or name.strip() == "":
                        name = f"Unknown Company {ticker}"
                        self.logger.warning(f"Empty company name for KOSPI ticker {ticker}, using fallback")
                    
                    company_info = {
                        'stock_symbol': ticker,  # Renamed from stock_code to stock_symbol
                        'stock_market': 'KOSPI',
                        'company_name': name.strip()
                    }
                    companies.append(company_info)
                    
                    # Log progress every 100 companies
                    if (i + 1) % 100 == 0:
                        self.logger.info(f"Processed {i + 1}/{len(kospi_tickers)} KOSPI companies")
                    
                except Exception as e:
                    self.logger.warning(f"Error getting info for KOSPI ticker {ticker}: {e}")
                    # Add fallback entry with basic info
                    company_info = {
                        'stock_symbol': ticker,
                        'stock_market': 'KOSPI',
                        'company_name': f"Unknown Company {ticker}"
                    }
                    companies.append(company_info)
                    continue
            
            # Process KOSDAQ tickers
            for i, ticker in enumerate(kosdaq_tickers):
                try:
                    # Get stock name from pykrx
                    name = stock.get_market_ticker_name(ticker)
                    
                    # Ensure company name is not empty
                    if not name or name.strip() == "":
                        name = f"Unknown Company {ticker}"
                        self.logger.warning(f"Empty company name for KOSDAQ ticker {ticker}, using fallback")
                    
                    company_info = {
                        'stock_symbol': ticker,  # Renamed from stock_code to stock_symbol
                        'stock_market': 'KOSDAQ',
                        'company_name': name.strip()
                    }
                    companies.append(company_info)
                    
                    # Log progress every 100 companies
                    if (i + 1) % 100 == 0:
                        self.logger.info(f"Processed {i + 1}/{len(kosdaq_tickers)} KOSDAQ companies")
                    
                except Exception as e:
                    self.logger.warning(f"Error getting info for KOSDAQ ticker {ticker}: {e}")
                    # Add fallback entry with basic info
                    company_info = {
                        'stock_symbol': ticker,
                        'stock_market': 'KOSDAQ',
                        'company_name': f"Unknown Company {ticker}"
                    }
                    companies.append(company_info)
                    continue
            
            self.logger.info(f"Retrieved {len(companies)} companies from pykrx (KOSPI + KOSDAQ)")
            return companies
            
        except Exception as e:
            self.logger.error(f"Error fetching data from pykrx: {e}")
            return []
    
    def get_ipo_date_from_pykrx(self, stock_symbol: str) -> Optional[datetime]:
        """
        Get IPO date for a specific stock symbol from pykrx.
        
        Args:
            stock_symbol: Stock symbol as string
            
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
                ticker=stock_symbol
            )
            
            if not df.empty:
                # Get the earliest date as proxy for IPO date
                earliest_date = df.index.min()
                return earliest_date.date()
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting IPO date for {stock_symbol}: {e}")
            return None
    
    def check_if_delisted(self, stock_symbol: str) -> Optional[datetime]:
        """
        Check if a stock has been delisted by verifying if the stock symbol stops appearing after a certain date.
        The delisting date is assumed to be the last date the symbol appears in pykrx data.

        Args:
            stock_symbol: Stock symbol as string

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
                ticker=stock_symbol
            )

            # If there is recent data, the stock is still active
            if not df_recent.empty:
                return None  # Stock is still active

            # If no recent data, check the last date the stock symbol appeared in the market
            # We'll look back from a reasonable start date (e.g., 1990-01-01)
            df_all = stock.get_market_ohlcv_by_date(
                fromdate="1990-01-01",
                todate=end_date.strftime("%Y-%m-%d"),
                ticker=stock_symbol
            )

            if not df_all.empty:
                # The last date the stock symbol appears is assumed to be the delisting date
                last_date = df_all.index.max()
                return last_date.date()
            else:
                # If the stock never appears, we cannot determine delisting date
                return None

        except Exception as e:
            self.logger.warning(f"Error checking delisting status for {stock_symbol}: {e}")
            return None
    def collect_and_update_stock_symbols(self) -> bool:
        """
        Collect all stock symbols with company names and update the database.
        This method ensures no duplicate rows are created by properly updating existing records.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Starting stock symbol collection and update")
            
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
                    stock_symbol = company['stock_symbol']
                    stock_market = company['stock_market']
                    company_name = company.get('company_name', 'Unknown Company')
                    
                    if (i + 1) % 100 == 0:
                        self.logger.info(f"Processing {i+1}/{len(companies)}: {company_name} ({stock_symbol}) - {stock_market}")
                    
                    # Check if record already exists
                    existing_record = session.query(KRStockCode).filter(
                        KRStockCode.stock_symbol == stock_symbol
                    ).first()
                    
                    if existing_record:
                        # Update existing record - only update fields that might have changed
                        updated = False
                        
                        # Update market if different
                        if existing_record.stock_market != stock_market:
                            existing_record.stock_market = stock_market
                            updated = True
                        
                        # Update company name if different
                        if existing_record.company_name != company_name:
                            existing_record.company_name = company_name
                            updated = True
                        
                        # Check delisting status
                        delisting_date = self.check_if_delisted(str(stock_symbol))
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
                        ipo_date = self.get_ipo_date_from_pykrx(str(stock_symbol))
                        
                        # Check delisting status
                        delisting_date = self.check_if_delisted(str(stock_symbol))
                        is_active = delisting_date is None
                        
                        new_record = KRStockCode(
                            stock_symbol=stock_symbol,
                            stock_market=stock_market,
                            company_name=company_name,
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
            
            self.logger.info(f"Stock symbol collection and update completed successfully")
            self.logger.info(f"Final statistics - Created: {created_count}, Updated: {updated_count}, Errors: {error_count}, Total processed: {len(companies)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in stock symbol collection: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def get_stock_symbols_from_db(self, market: str = None, active_only: bool = True) -> pd.DataFrame:
        """
        Retrieve stock symbols from database.
        
        Args:
            market: Filter by market (KOSPI or KOSDAQ)
            active_only: If True, only return active (non-delisted) stocks
            
        Returns:
            DataFrame containing stock symbols
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
                    'stock_symbol': record.stock_symbol,
                    'stock_market': record.stock_market,
                    'company_name': record.company_name,
                    'ipo_date': record.ipo_date,
                    'delisting_date': record.delisting_date,
                    'is_active': record.is_active,
                    'created_at': record.created_at,
                    'updated_at': record.updated_at
                })
            
            session.close()
            
            df = pd.DataFrame(data)
            self.logger.info(f"Retrieved {len(df)} stock symbols from database")
            return df
            
        except Exception as e:
            self.logger.error(f"Error retrieving stock symbols from database: {e}")
            session.close()
            return pd.DataFrame()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the stock symbols in the database.
        
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
        Save stock symbols data to CSV file in the data/raw/korean_stocks directory.
        
        Args:
            market: Filter by market (KOSPI or KOSDAQ)
            active_only: If True, only save active (non-delisted) stocks
            filename: Custom filename (optional)
            
        Returns:
            Path to the saved CSV file
        """
        try:
            # Get data from database
            df = self.get_stock_symbols_from_db(market, active_only)
            
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
                filename = f"kr_stock_symbols{market_suffix}{active_suffix}_{timestamp}.csv"
            
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
            all_stocks_path = self.save_to_csv(active_only=False, filename="kr_stock_symbols_all.csv")
            if all_stocks_path:
                saved_files['all_stocks'] = all_stocks_path
            
            # Save active stocks only
            active_stocks_path = self.save_to_csv(active_only=True, filename="kr_stock_symbols_active.csv")
            if active_stocks_path:
                saved_files['active_stocks'] = active_stocks_path
            
            # Save KOSPI stocks
            kospi_path = self.save_to_csv(market="KOSPI", filename="kr_stock_symbols_kospi.csv")
            if kospi_path:
                saved_files['kospi_stocks'] = kospi_path
            
            # Save KOSDAQ stocks
            kosdaq_path = self.save_to_csv(market="KOSDAQ", filename="kr_stock_symbols_kosdaq.csv")
            if kosdaq_path:
                saved_files['kosdaq_stocks'] = kosdaq_path
            
            # Save statistics
            stats = self.get_statistics()
            if stats:
                stats_df = pd.DataFrame([stats])
                data_dir = Path("data/raw/korean_stocks")
                stats_path = data_dir / "kr_stock_symbols_statistics.csv"
                stats_df.to_csv(stats_path, index=False, encoding='utf-8-sig')
                saved_files['statistics'] = str(stats_path)
            
            self.logger.info(f"All data saved to: {data_dir}")
            return saved_files
            
        except Exception as e:
            self.logger.error(f"Error saving all formats: {e}")
            return {}


# Convenience functions
def collect_kr_stock_symbols(database_url: str = None) -> bool:
    """Collect and update Korean stock symbols with company names."""
    collector = KRStockCodeCollector(database_url)
    return collector.collect_and_update_stock_symbols()

def cleanup_kr_stock_symbols_duplicates(database_url: str = None) -> int:
    """Clean up duplicate Korean stock symbols from database."""
    collector = KRStockCodeCollector(database_url)
    return collector.cleanup_duplicates()

def get_kr_stock_symbols(market: str = None, active_only: bool = True, database_url: str = None) -> pd.DataFrame:
    """Get Korean stock symbols from database."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_stock_symbols_from_db(market, active_only)

def get_kr_stock_symbols_statistics(database_url: str = None) -> Dict[str, Any]:
    """Get statistics about Korean stock symbols."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_statistics()

def save_kr_stock_symbols_to_csv(market: str = None, active_only: bool = True, 
                                filename: str = None, database_url: str = None) -> str:
    """Save Korean stock symbols to CSV file."""
    collector = KRStockCodeCollector(database_url)
    return collector.save_to_csv(market, active_only, filename)

def save_kr_stock_symbols_all_formats(database_url: str = None) -> Dict[str, str]:
    """Save Korean stock symbols in all formats (CSV files)."""
    collector = KRStockCodeCollector(database_url)
    return collector.save_all_formats()

def get_database_info(database_url: str = None) -> Dict[str, Any]:
    """Get PostgreSQL database connection information."""
    collector = KRStockCodeCollector(database_url)
    return collector.get_database_info()

def run_database_maintenance(database_url: str = None) -> bool:
    """Run PostgreSQL database maintenance (VACUUM ANALYZE)."""
    collector = KRStockCodeCollector(database_url)
    collector.vacuum_and_analyze()
    return True
