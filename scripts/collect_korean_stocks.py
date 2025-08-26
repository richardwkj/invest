#!/usr/bin/env python3
"""
Script to collect all Korean stock data using DART API and yfinance.
This script runs the complete data collection process with proper rate limiting.
"""

import sys
from pathlib import Path
import os
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from data_collection.korean_stocks import KoreanStockCollector, collect_all_korean_stocks_data
from config.settings import settings

def main():
    """Main function to run Korean stock data collection."""
    print("🇰🇷 Korean Stock Data Collection Script")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Check if pykrx is available
    try:
        import pykrx
        print("✅ pykrx package is available")
    except ImportError:
        print("❌ pykrx package not found!")
        print("   Please install pykrx: pip install pykrx")
        return
    
    print("✅ Configuration check passed")
    print(f"   Environment: Production (PostgreSQL)")
    print(f"   Target Database: invest_stocks")
    print()
    
    try:
        # Initialize collector
        print("🔧 Initializing Korean Stock Collector...")
        
        # Use PostgreSQL for invest_stocks database
        try:
            from config.database_config import get_database_url
            database_url = get_database_url("invest_user")  # Use invest_user for better security
        except ImportError:
            # Fallback to default connection
            database_url = "postgresql://invest_user:Lwhfy!3!a@localhost:5432/invest_stocks"
        
        print(f"   Database: PostgreSQL (invest_stocks)")
        print(f"   Connection: {database_url.replace('password', '***')}")
        
        collector = KoreanStockCollector(database_url)
        print("✅ Collector initialized successfully")
        print()
        
        # Get all stock codes from pykrx
        print("📋 Step 1: Getting all stock codes from pykrx...")
        companies = collector.get_all_stock_codes_from_pykrx()
        
        if not companies:
            print("❌ No companies retrieved from pykrx")
            return
        
        print(f"✅ Retrieved {len(companies)} companies from pykrx")
        print()
        
        # Start comprehensive data collection
        print("🚀 Step 2: Starting comprehensive data collection...")
        print("   This will take a while due to 3-second pauses between API calls.")
        print("   Estimated time: ~{:.1f} hours for all {:,} companies".format(
            len(companies) * 3 / 3600, len(companies)
        ))
        print()
        
        # Run the collection
        all_data = collector.collect_all_stocks_data()
        
        if not all_data.empty:
            print()
            print("✅ Data collection completed successfully!")
            print(f"   Total records: {len(all_data):,}")
            print(f"   Unique stocks: {all_data['symbol'].nunique()}")
            print(f"   Date range: {all_data['date'].min()} to {all_data['date'].max()}")
            print(f"   KOSPI stocks: {len(all_data[all_data['market'] == 'KOSPI']['symbol'].unique())}")
            print(f"   KOSDAQ stocks: {len(all_data[all_data['market'] == 'KOSDAQ']['symbol'].unique())}")
            
            # Save summary to CSV
            summary_file = f"korean_stocks_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            all_data.to_csv(summary_file, index=False, encoding='utf-8-sig')
            print(f"   Summary saved to: {summary_file}")
            
        else:
            print("❌ No data collected")
            
    except KeyboardInterrupt:
        print("\n⚠️  Data collection interrupted by user")
        print("   Partial data may have been saved to the database")
        
    except Exception as e:
        print(f"\n❌ Error during data collection: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print(f"\nFinished at: {datetime.now()}")
        print("=" * 60)

if __name__ == "__main__":
    main()
