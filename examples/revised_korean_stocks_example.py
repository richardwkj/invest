#!/usr/bin/env python3
"""
Example script for revised Korean stock data collection with DART API and PostgreSQL.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from data_collection.korean_stocks import KoreanStockCollector, collect_all_korean_stocks_data
import pandas as pd

def main():
    """Main example function."""
    print("🇰🇷 Revised Korean Stock Data Collection Example")
    print("=" * 60)
    
    # Example 1: Get all stock codes from DART API
    print("\n1. 📋 Getting All Stock Codes from DART API")
    print("-" * 50)
    
    collector = KoreanStockCollector()
    companies = collector.get_all_stock_codes_from_dart()
    
    if companies:
        print(f"✅ Retrieved {len(companies)} companies from DART API")
        print("\nSample companies:")
        for i, company in enumerate(companies[:5]):
            print(f"   {i+1}. {company.get('corp_name', 'Unknown')} ({company.get('stock_code', 'N/A')})")
    else:
        print("❌ No companies retrieved. Make sure DART_API_KEY is set.")
        return
    
    # Example 2: Get maximum historical data for a specific stock
    print("\n\n2. 📈 Getting Maximum Historical Data")
    print("-" * 50)
    
    # Test with Samsung Electronics
    samsung_data = collector.get_stock_data_with_max_history("005930", "KOSPI")
    
    if not samsung_data.empty:
        print(f"✅ Samsung Electronics: {len(samsung_data)} records")
        print(f"   Date range: {samsung_data['date'].min()} to {samsung_data['date'].max()}")
        print(f"   Latest close price: {samsung_data['close'].iloc[-1]:,.0f} KRW")
        print(f"   Latest volume: {samsung_data['volume'].iloc[-1]:,}")
        
        # Show recent data
        print("\nRecent data:")
        recent_data = samsung_data[['date', 'open_price', 'high', 'low', 'close', 'volume']].tail(5)
        print(recent_data.to_string(index=False))
    else:
        print("❌ No data retrieved for Samsung Electronics")
    
    # Example 3: Database operations
    print("\n\n3. 💾 Database Operations")
    print("-" * 50)
    
    # Use SQLite for this example
    db_collector = KoreanStockCollector("sqlite:///./example_korean_stocks.db")
    
    if not samsung_data.empty:
        # Save to database
        success = db_collector.save_stock_data_to_db(samsung_data)
        
        if success:
            print("✅ Data saved to database successfully")
            
            # Retrieve from database
            retrieved_data = db_collector.get_stock_data_from_db(
                symbol='005930', 
                start_date='2024-01-01'
            )
            
            if not retrieved_data.empty:
                print(f"✅ Retrieved {len(retrieved_data)} records from database")
                print(f"   Date range: {retrieved_data['date'].min()} to {retrieved_data['date'].max()}")
            else:
                print("❌ No data retrieved from database")
        else:
            print("❌ Failed to save data to database")
    
    # Example 4: Comprehensive data collection (limited scope)
    print("\n\n4. 🚀 Comprehensive Data Collection (Limited)")
    print("-" * 50)
    
    print("This would collect data for ALL Korean stocks with 3-second pauses.")
    print("For this example, we'll show the process with a small sample:")
    
    # Get first 3 companies for demonstration
    sample_companies = companies[:3]
    
    all_stock_data = []
    
    for i, company in enumerate(sample_companies):
        stock_code = company.get('stock_code', '').strip()
        corp_name = company.get('corp_name', '').strip()
        
        if not stock_code or len(stock_code) != 6:
            continue
        
        # Determine market
        market = "KOSPI" if stock_code.startswith('0') else "KOSDAQ"
        
        print(f"\nProcessing {i+1}/{len(sample_companies)}: {corp_name} ({stock_code}) - {market}")
        
        # Get stock data
        stock_df = db_collector.get_stock_data_with_max_history(stock_code, market)
        
        if not stock_df.empty:
            stock_df['longname'] = corp_name
            all_stock_data.append(stock_df)
            
            # Save to database
            db_collector.save_stock_data_to_db(stock_df)
            
            print(f"   ✅ Collected {len(stock_df)} records")
        else:
            print(f"   ❌ No data available")
        
        # 3-second pause (simulated)
        print("   ⏳ Waiting 3 seconds... (simulated)")
    
    if all_stock_data:
        combined_df = pd.concat(all_stock_data, ignore_index=True)
        print(f"\n✅ Total records collected: {len(combined_df)}")
        print(f"   Unique stocks: {combined_df['symbol'].nunique()}")
        print(f"   Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    
    # Example 5: Database queries
    print("\n\n5. 🔍 Database Queries")
    print("-" * 50)
    
    # Get all KOSPI stocks from database
    kospi_data = db_collector.get_stock_data_from_db(market='KOSPI')
    
    if not kospi_data.empty:
        print(f"✅ KOSPI stocks in database: {kospi_data['symbol'].nunique()}")
        print(f"   Total records: {len(kospi_data)}")
        
        # Get latest data for each stock
        latest_data = kospi_data.groupby('symbol').last().reset_index()
        print(f"\nLatest prices:")
        for _, row in latest_data.iterrows():
            print(f"   {row['longname']} ({row['symbol']}): {row['close']:,.0f} KRW")
    else:
        print("❌ No KOSPI data in database")
    
    print("\n" + "=" * 60)
    print("✅ Example completed successfully!")
    print("\n📝 Key Features:")
    print("   - DART API integration for complete stock list")
    print("   - Maximum historical data with 1-day intervals")
    print("   - 3-second pauses to prevent API overloading")
    print("   - PostgreSQL database storage")
    print("   - Comprehensive error handling")
    
    print("\n🚀 To run full collection:")
    print("   collect_all_korean_stocks_data()")

if __name__ == "__main__":
    main()
