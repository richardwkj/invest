#!/usr/bin/env python3
"""
Test script to verify Korean stock codes data saving to data/raw/korean_stocks directory.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent / "src"))

from data_collection.kr_stock_codes import KRStockCodeCollector, save_kr_stock_codes_all_formats
import pandas as pd

def main():
    """Test data saving functionality."""
    print("🇰🇷 Korean Stock Codes Data Saving Test")
    print("=" * 60)
    
    try:
        # Initialize collector (will automatically save to data/raw/korean_stocks)
        print("🔧 Initializing KR Stock Code Collector...")
        collector = KRStockCodeCollector()  # Uses default path: data/raw/korean_stocks/kr_stock_codes.db
        print("✅ Collector initialized successfully")
        print(f"   Database: {collector.database_url}")
        print()
        
        # Get stock codes from pykrx (limited sample for testing)
        print("📋 Getting stock codes from pykrx...")
        companies = collector.get_all_stock_codes_from_pykrx()
        
        if not companies:
            print("❌ No companies retrieved from pykrx")
            return
        
        print(f"✅ Retrieved {len(companies)} companies from pykrx")
        
        # Process only first 5 companies for testing
        test_companies = companies[:5]
        print(f"   Processing {len(test_companies)} companies for testing")
        
        # Save sample data to database
        session = collector.SessionLocal()
        
        for i, company in enumerate(test_companies):
            try:
                stock_code = company['stock_code']
                stock_market = company['stock_market']
                
                print(f"   Processing {i+1}/{len(test_companies)}: {stock_code} ({stock_market})")
                
                # Get IPO date
                ipo_date = collector.get_ipo_date_from_pykrx(str(stock_code))
                
                # Check delisting status
                delisting_date = collector.check_if_delisted(str(stock_code))
                is_active = delisting_date is None
                
                # Create record
                from data_collection.kr_stock_codes import KRStockCode
                from datetime import datetime
                
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
                
            except Exception as e:
                print(f"   ❌ Error processing {company.get('stock_code', 'Unknown')}: {e}")
                continue
        
        # Commit changes
        session.commit()
        session.close()
        
        print("✅ Sample data saved to database")
        
        # Test CSV saving functionality
        print("\n💾 Testing CSV saving functionality...")
        
        # Save all formats
        saved_files = collector.save_all_formats()
        
        if saved_files:
            print("✅ Files saved successfully:")
            for file_type, file_path in saved_files.items():
                print(f"   - {file_type}: {file_path}")
                
                # Verify file exists
                if Path(file_path).exists():
                    print(f"     ✓ File exists")
                    # Show file size
                    file_size = Path(file_path).stat().st_size
                    print(f"     ✓ File size: {file_size} bytes")
                else:
                    print(f"     ❌ File not found")
        else:
            print("❌ No files saved")
        
        # Check data directory structure
        print("\n📁 Data directory structure:")
        data_dir = Path("data/raw/korean_stocks")
        if data_dir.exists():
            print(f"   Directory: {data_dir}")
            print("   Files:")
            for file in data_dir.iterdir():
                if file.is_file():
                    file_size = file.stat().st_size
                    print(f"     - {file.name} ({file_size} bytes)")
        else:
            print("   ❌ Data directory not found")
        
        print("\n" + "=" * 60)
        print("✅ Data saving test completed successfully!")
        print(f"   Data saved to: {data_dir}")
        
    except Exception as e:
        print(f"\n❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
