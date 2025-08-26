#!/usr/bin/env python3
"""
Script to collect Korean stock data using Kiwoom API.
This script uses the Kiwoom test server to collect historical stock price data.
"""

import sys
import os
import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import warnings

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from data_collection.kr_stock_codes import KRStockCodeCollector
from utils.logger import get_logger
from config.kiwoom_config import get_kiwoom_host, get_rate_limit_delay

# Suppress warnings
warnings.filterwarnings("ignore")

class KiwoomStockCollector:
    """
    Collects Korean stock data using Kiwoom API.
    Uses test server for development and testing.
    """
    
    def __init__(self, app_key: str, secret_key: str, use_test_server: bool = True):
        """
        Initialize the Kiwoom stock collector.
        
        Args:
            app_key: Kiwoom application key
            secret_key: Kiwoom secret key
            use_test_server: Whether to use test server (default: True)
        """
        self.app_key = app_key
        self.secret_key = secret_key
        self.use_test_server = use_test_server
        
        # Set host based on server type
        self.host = get_kiwoom_host()
        if use_test_server:
            print("🧪 Using Kiwoom TEST server")
        else:
            print("🚀 Using Kiwoom PRODUCTION server")
        
        # Initialize logger
        self.logger = get_logger(__name__)
        
        # Get access token
        self.access_token = None
        self._get_access_token()
    
    def _get_access_token(self) -> bool:
        """
        Get access token from Kiwoom API.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            endpoint = '/oauth2/token'
            url = self.host + endpoint
            
            headers = {
                'Content-Type': 'application/json;charset=UTF-8',
            }
            
            data = {
                'grant_type': 'client_credentials',
                'appkey': self.app_key,
                'secretkey': self.secret_key,
            }
            
            self.logger.info("Requesting access token from Kiwoom API...")
            response = requests.post(url, headers=headers, json=data, timeout=30)
            
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('return_code') == 0:
                    self.access_token = response_data.get('token')  # Kiwoom uses 'token' not 'access_token'
                    self.logger.info("✅ Access token obtained successfully")
                    return True
                else:
                    self.logger.error(f"❌ Failed to get access token: {response_data.get('return_msg')}")
                    return False
            else:
                self.logger.error(f"❌ HTTP error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error getting access token: {e}")
            return False
    
    def get_stock_price_history(self, stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Get historical stock price data for a specific stock.
        
        Args:
            stock_code: Stock code (e.g., '005930' for Samsung)
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            pd.DataFrame: Historical price data or None if failed
        """
        if not self.access_token:
            self.logger.error("❌ No access token available")
            return None
        
        try:
            endpoint = '/uapi/domestic-stock/v1/quotations/inquire-price'
            url = self.host + endpoint
            
            headers = {
                'Content-Type': 'application/json;charset=UTF-8',
                'authorization': f'Bearer {self.access_token}',
                'appkey': self.app_key,
                'appsecret': self.secret_key,
                'tr_id': 'FHKST01010100',  # Historical price inquiry
            }
            
            params = {
                'FID_COND_MRKT_DIV_CODE': 'J',  # KOSPI
                'FID_COND_SCR_DIV_CODE': '20171',  # Stock
                'FID_INPUT_ISCD': stock_code,
                'FID_INPUT_DATE_1': start_date,
                'FID_INPUT_DATE_2': end_date,
                'FID_VOL_CNT': '100',  # Number of records
            }
            
            self.logger.info(f"📊 Fetching price data for {stock_code} from {start_date} to {end_date}")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                response_data = response.json()
                
                if response_data.get('return_code') == 0:
                    # Parse the response data
                    stock_data = response_data.get('daly_stkpc', [])
                    
                    if stock_data:
                        # Convert to DataFrame
                        df = pd.DataFrame(stock_data)
                        
                        # Clean and standardize column names
                        df = self._clean_stock_data(df, stock_code)
                        
                        self.logger.info(f"✅ Retrieved {len(df)} records for {stock_code}")
                        return df
                    else:
                        self.logger.warning(f"⚠️ No data returned for {stock_code}")
                        return None
                else:
                    self.logger.error(f"❌ API error for {stock_code}: {response_data.get('return_msg')}")
                    return None
            else:
                self.logger.error(f"❌ HTTP error {response.status_code} for {stock_code}: {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error fetching data for {stock_code}: {e}")
            return None
    
    def _clean_stock_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        Clean and standardize stock data from Kiwoom API.
        
        Args:
            df: Raw DataFrame from API
            stock_code: Stock code for reference
            
        Returns:
            pd.DataFrame: Cleaned and standardized data
        """
        try:
            # Add stock code column
            df['stock_code'] = stock_code
            
            # Convert date format from YYYYMMDD to YYYY-MM-DD
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.date
            
            # Clean price columns (remove +/- signs and convert to numeric)
            price_columns = ['open_pric', 'high_pric', 'low_pric', 'close_pric', 'pred_rt']
            for col in price_columns:
                if col in df.columns:
                    df[col] = df[col].str.replace('+', '').str.replace('-', '-')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Clean volume and amount columns
            volume_columns = ['trde_qty', 'amt_mn']
            for col in volume_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Clean percentage columns
            percentage_columns = ['flu_rt', 'crd_rt', 'for_rt', 'for_poss', 'for_wght']
            for col in percentage_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Rename columns to standard format
            column_mapping = {
                'open_pric': 'open_price',
                'high_pric': 'high_price',
                'low_pric': 'low_price',
                'close_pric': 'close_price',
                'pred_rt': 'price_change',
                'flu_rt': 'fluctuation_rate',
                'trde_qty': 'volume',
                'amt_mn': 'amount',
                'crd_rt': 'credit_rate',
                'for_rt': 'foreign_rate',
                'for_poss': 'foreign_possession',
                'for_wght': 'foreign_weight'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Reorder columns for better readability
            priority_columns = ['stock_code', 'date', 'open_price', 'high_price', 'low_price', 
                              'close_price', 'price_change', 'fluctuation_rate', 'volume', 'amount']
            
            # Add any missing priority columns
            for col in priority_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder columns
            available_columns = [col for col in priority_columns if col in df.columns]
            remaining_columns = [col for col in df.columns if col not in priority_columns]
            df = df[available_columns + remaining_columns]
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error cleaning data for {stock_code}: {e}")
            return df
    
    def collect_multiple_stocks(self, stock_codes: List[str], start_date: str, end_date: str, 
                              delay: float = 1.0) -> Dict[str, pd.DataFrame]:
        """
        Collect historical data for multiple stocks with rate limiting.
        
        Args:
            stock_codes: List of stock codes to collect
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            delay: Delay between API calls in seconds
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping stock codes to their data
        """
        results = {}
        total_stocks = len(stock_codes)
        
        self.logger.info(f"🚀 Starting collection for {total_stocks} stocks")
        self.logger.info(f"   Date range: {start_date} to {end_date}")
        self.logger.info(f"   Delay between calls: {delay} seconds")
        
        for i, stock_code in enumerate(stock_codes, 1):
            try:
                self.logger.info(f"📊 Processing {i}/{total_stocks}: {stock_code}")
                
                # Get stock data
                stock_data = self.get_stock_price_history(stock_code, start_date, end_date)
                
                if stock_data is not None and not stock_data.empty:
                    results[stock_code] = stock_data
                    self.logger.info(f"✅ Successfully collected data for {stock_code}")
                else:
                    self.logger.warning(f"⚠️ No data collected for {stock_code}")
                
                # Rate limiting
                if i < total_stocks:
                    self.logger.info(f"⏳ Waiting {delay} seconds before next request...")
                    time.sleep(delay)
                    
            except Exception as e:
                self.logger.error(f"❌ Error processing {stock_code}: {e}")
                continue
        
        self.logger.info(f"🎯 Collection completed. Successfully collected data for {len(results)} stocks")
        return results
    
    def save_to_csv(self, data_dict: Dict[str, pd.DataFrame], output_dir: str = "data/raw/korean_stocks/kiwoom"):
        """
        Save collected data to CSV files.
        
        Args:
            data_dict: Dictionary of stock data
            output_dir: Output directory for CSV files
        """
        try:
            # Create output directory if it doesn't exist
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Save individual stock files
            for stock_code, df in data_dict.items():
                if df is not None and not df.empty:
                    filename = f"{output_dir}/{stock_code}_kiwoom_data.csv"
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    self.logger.info(f"💾 Saved {stock_code} data to {filename}")
            
            # Save combined data
            if data_dict:
                combined_df = pd.concat(data_dict.values(), ignore_index=True)
                combined_filename = f"{output_dir}/combined_kiwoom_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                combined_df.to_csv(combined_filename, index=False, encoding='utf-8-sig')
                self.logger.info(f"💾 Saved combined data to {combined_filename}")
                
                # Print summary
                print(f"\n📊 Data Collection Summary:")
                print(f"   Total stocks processed: {len(data_dict)}")
                print(f"   Total records: {len(combined_df):,}")
                print(f"   Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
                print(f"   Output directory: {output_dir}")
                
        except Exception as e:
            self.logger.error(f"❌ Error saving data to CSV: {e}")


def main():
    """Main function to run Kiwoom stock data collection."""
    print("🇰🇷 Kiwoom Korean Stock Data Collection Script")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Configuration - Use configuration file
    from config.kiwoom_config import get_kiwoom_credentials, is_test_server
    app_key, secret_key = get_kiwoom_credentials()
    use_test_server = is_test_server()
    
    print("🔧 Configuration:")
    print(f"   App Key: {app_key[:20]}...")
    print(f"   Secret Key: {secret_key[:20]}...")
    print(f"   Test Server: {use_test_server}")
    print()
    
    try:
        # Initialize collector
        print("🔧 Initializing Kiwoom Stock Collector...")
        collector = KiwoomStockCollector(app_key, secret_key, use_test_server)
        
        if not collector.access_token:
            print("❌ Failed to initialize collector - no access token")
            return
        
        print("✅ Collector initialized successfully")
        print()
        
        # Get stock codes from existing database
        print("📋 Getting stock codes from database...")
        try:
            stock_collector = KRStockCodeCollector()
            stock_codes_df = stock_collector.get_stock_codes_from_db(active_only=True)
            
            if stock_codes_df.empty:
                print("❌ No stock codes found in database")
                return
            
            # Get first 5 stocks for testing (you can modify this)
            test_stock_codes = stock_codes_df['stock_code'].head(5).tolist()
            print(f"✅ Retrieved {len(test_stock_codes)} stock codes for testing")
            print(f"   Test stocks: {', '.join(test_stock_codes)}")
            
        except Exception as e:
            print(f"❌ Error getting stock codes: {e}")
            # Use sample stock codes for testing
            test_stock_codes = ['005930', '000660', '035420', '051910', '006400']  # Samsung, SK Hynix, NAVER, LG Chem, Seegene
            print(f"⚠️ Using sample stock codes: {', '.join(test_stock_codes)}")
        
        print()
        
        # Set date range (last 30 days for testing)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        print(f"📅 Date range: {start_date_str} to {end_date_str}")
        print()
        
        # Collect data
        print("🚀 Starting data collection...")
        print("   This will take a while due to rate limiting.")
        print(f"   Estimated time: ~{len(test_stock_codes) * 1 / 60:.1f} minutes for {len(test_stock_codes)} stocks")
        print()
        
        # Collect data for test stocks
        results = collector.collect_multiple_stocks(
            test_stock_codes, 
            start_date_str, 
            end_date_str, 
            delay=1.0
        )
        
        if results:
            print()
            print("✅ Data collection completed successfully!")
            
            # Save to CSV
            collector.save_to_csv(results)
            
        else:
            print("❌ No data collected")
            
    except KeyboardInterrupt:
        print("\n⚠️  Data collection interrupted by user")
        
    except Exception as e:
        print(f"\n❌ Error during data collection: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print(f"\nFinished at: {datetime.now()}")
        print("=" * 60)


if __name__ == "__main__":
    main()
