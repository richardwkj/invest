#!/usr/bin/env python3
"""
Script to collect and update all Korean stock codes with IPO and delisting dates.
This script creates and maintains the KR_stock_codes table.
"""

import sys
from pathlib import Path
import os
from datetime import datetime

# Add the src and root directories to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))
sys.path.append(str(Path(__file__).parent.parent))


from data_collection.kr_stock_codes import KRStockCodeCollector, collect_kr_stock_symbols, get_kr_stock_symbols_statistics

# Try to import settings, but use defaults if not available
try:
    from config.settings import settings
except ImportError:
    # Create a simple settings object if config is not available
    class SimpleSettings:
        debug = True
        database_url = None
    
    settings = SimpleSettings()

def main():
    """Main function to run Korean stock codes collection."""
    print("🇰🇷 Korean Stock Codes Collection Script")
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
        print("🔧 Initializing KR Stock Code Collector...")
        
        # Use PostgreSQL for invest_stocks database
        try:
            from config.database_config import get_database_url
            database_url = get_database_url("invest_user")  # Use invest_user for better security
        except ImportError:
            # Fallback to default connection
            database_url = "postgresql://invest_user:Lwhfy!3!a@localhost:5432/invest_stocks"
        
        print(f"   Database: PostgreSQL (invest_stocks)")
        print(f"   Connection: {database_url.replace('password', '***')}")
        
        collector = KRStockCodeCollector(database_url)
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
        
        # Show market distribution
        kospi_count = len([c for c in companies if c['stock_market'] == 'KOSPI'])
        kosdaq_count = len([c for c in companies if c['stock_market'] == 'KOSDAQ'])
        print(f"📊 Market Distribution:")
        print(f"   KOSPI: {kospi_count} companies")
        print(f"   KOSDAQ: {kosdaq_count} companies")
        print()
        
        # Start comprehensive collection
        print("🚀 Step 2: Starting comprehensive stock codes collection...")
        print("   This will collect IPO dates and check delisting status for all companies.")
        print("   Estimated time: ~{:.1f} minutes for all {:,} companies".format(
            len(companies) * 2 / 60, len(companies)  # ~2 seconds per company
        ))
        print()
        
        # Run the collection
        success = collector.collect_and_update_stock_symbols()
        
        if success:
            print()
            print("✅ Stock codes collection completed successfully!")
            
            # Get and display statistics
            print("\n📈 Final Statistics:")
            stats = collector.get_statistics()
            
            if stats:
                print(f"   Total stocks: {stats.get('total_stocks', 0):,}")
                print(f"   KOSPI stocks: {stats.get('kospi_stocks', 0):,}")
                print(f"   KOSDAQ stocks: {stats.get('kosdaq_stocks', 0):,}")
                print(f"   Active stocks: {stats.get('active_stocks', 0):,}")
                print(f"   Delisted stocks: {stats.get('delisted_stocks', 0):,}")
                print(f"   With IPO date: {stats.get('with_ipo_date', 0):,}")
                
                if stats.get('earliest_ipo'):
                    print(f"   Earliest IPO: {stats['earliest_ipo']}")
                if stats.get('latest_ipo'):
                    print(f"   Latest IPO: {stats['latest_ipo']}")
            
            # Save all data formats to data/raw/korean_stocks directory
            print("\n💾 Saving data to CSV files...")
            saved_files = collector.save_all_formats()
            
            if saved_files:
                print("   Files saved:")
                for file_type, file_path in saved_files.items():
                    print(f"     - {file_type}: {file_path}")
            else:
                print("   No files saved")
            
            print(f"\n📋 Database Table: kr_stock_codes")
            print(f"   Columns: stock_symbol, stock_market, company_name, ipo_date, delisting_date, is_active")
            
        else:
            print("❌ Stock codes collection failed")
            
    except KeyboardInterrupt:
        print("\n⚠️  Stock codes collection interrupted by user")
        print("   Partial data may have been saved to the database")
        
    except Exception as e:
        print(f"\n❌ Error during stock codes collection: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print(f"\nFinished at: {datetime.now()}")
        print("=" * 60)

if __name__ == "__main__":
    main()
