#!/usr/bin/env python3
"""
Example script for Korean stock market data collection.
This script shows basic usage of the KoreanStockCollector class.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from data_collection.korean_stocks import KoreanStockCollector
import pandas as pd


def main():
    """Main example function."""
    print("🇰🇷 Korean Stock Market Data Collection Example")
    print("=" * 50)
    
    # Initialize the collector
    collector = KoreanStockCollector()
    
    # Example 1: Get stock information
    print("\n1. 📊 Getting Stock Information")
    print("-" * 30)
    
    # Samsung Electronics
    samsung_info = collector.get_stock_info("005930", "KOSPI")
    if samsung_info:
        print(f"📱 Samsung Electronics:")
        print(f"   Name: {samsung_info.get('longName', 'Unknown')}")
        print(f"   Symbol: {samsung_info.get('symbol', 'N/A')}")
        print(f"   Market Cap: {samsung_info.get('marketCap', 'N/A'):,}")
        print(f"   Current Price: {samsung_info.get('currentPrice', 'N/A')} KRW")
        print(f"   P/E Ratio: {samsung_info.get('trailingPE', 'N/A')}")
        print(f"   Sector: {samsung_info.get('sector', 'N/A')}")
    
    # NAVER
    naver_info = collector.get_stock_info("035420", "KOSPI")
    if naver_info:
        print(f"\n🌐 NAVER:")
        print(f"   Name: {naver_info.get('longName', 'Unknown')}")
        print(f"   Current Price: {naver_info.get('currentPrice', 'N/A')} KRW")
        print(f"   Market Cap: {naver_info.get('marketCap', 'N/A'):,}")
    
    # Example 2: Get historical data
    print("\n\n2. 📈 Getting Historical Data")
    print("-" * 30)
    
    # Get Samsung Electronics historical data (last 30 days)
    samsung_data = collector.get_stock_data("005930", "KOSPI", period="1mo")
    
    if not samsung_data.empty:
        print(f"📱 Samsung Electronics - Last 30 days:")
        print(f"   Records: {len(samsung_data)}")
        
        # Show latest data
        latest = samsung_data.iloc[-1]
        print(f"   Latest Date: {latest['Date'].strftime('%Y-%m-%d')}")
        print(f"   Close Price: {latest['Close']:,.0f} KRW")
        print(f"   Volume: {latest['Volume']:,}")
        print(f"   High: {latest['High']:,.0f} KRW")
        print(f"   Low: {latest['Low']:,.0f} KRW")
        
        # Calculate price change
        first = samsung_data.iloc[0]
        price_change = latest['Close'] - first['Close']
        price_change_pct = (price_change / first['Close']) * 100
        print(f"   30-day Change: {price_change:+,.0f} KRW ({price_change_pct:+.2f}%)")
    
    # Example 3: Get market summary
    print("\n\n3. 📊 Getting Market Summary")
    print("-" * 30)
    
    # KOSPI market summary
    kospi_summary = collector.get_market_summary("KOSPI")
    if kospi_summary:
        print(f"📈 KOSPI Market Summary:")
        print(f"   Current Index: {kospi_summary.get('current_price', 'N/A'):,.0f}")
        print(f"   Daily Change: {kospi_summary.get('change', 'N/A'):+,.0f}")
        print(f"   Change %: {kospi_summary.get('change_percent', 'N/A'):+.2f}%")
        print(f"   Volume: {kospi_summary.get('volume', 'N/A'):,}")
    
    # KOSDAQ market summary
    kosdaq_summary = collector.get_market_summary("KOSDAQ")
    if kosdaq_summary:
        print(f"\n📊 KOSDAQ Market Summary:")
        print(f"   Current Index: {kosdaq_summary.get('current_price', 'N/A'):,.0f}")
        print(f"   Daily Change: {kosdaq_summary.get('change', 'N/A'):+,.0f}")
        print(f"   Change %: {kosdaq_summary.get('change_percent', 'N/A'):+.2f}%")
    
    # Example 4: Get top stocks
    print("\n\n4. 🏆 Getting Top Stocks")
    print("-" * 30)
    
    # Get top 5 KOSPI stocks by market cap
    top_stocks = collector.get_top_stocks("KOSPI", top_n=5)
    
    if not top_stocks.empty:
        print(f"🏆 Top 5 KOSPI stocks by market cap:")
        for idx, row in top_stocks.iterrows():
            print(f"   {idx+1}. {row.get('longName', row.get('symbol', 'Unknown'))}")
            print(f"      Symbol: {row.get('symbol', 'N/A')}")
            print(f"      Market Cap: {row.get('marketCap', 'N/A'):,}")
            print(f"      Price: {row.get('currentPrice', 'N/A'):,.0f} KRW")
    
    # Example 5: Save data to CSV
    print("\n\n5. 💾 Saving Data to CSV")
    print("-" * 30)
    
    # Get Samsung Electronics data and save to CSV
    samsung_data = collector.get_stock_data("005930", "KOSPI", period="1mo")
    
    if not samsung_data.empty:
        output_path = "data/raw/korean_stocks/samsung_electronics_example.csv"
        collector.save_to_csv(samsung_data, output_path)
        print(f"✅ Samsung Electronics data saved to: {output_path}")
        print(f"   Records: {len(samsung_data)}")
        print(f"   Date range: {samsung_data['Date'].min()} to {samsung_data['Date'].max()}")
    
    print("\n" + "=" * 50)
    print("✅ Example completed successfully!")
    print("\n📝 Next steps:")
    print("   - Run the test script: python scripts/test_korean_stocks.py")
    print("   - Explore the saved CSV files in data/raw/korean_stocks/")
    print("   - Integrate with your analysis pipeline")


if __name__ == "__main__":
    main()
