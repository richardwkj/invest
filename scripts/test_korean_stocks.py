#!/usr/bin/env python3
"""
Test script for Korean stock market data collection using yfinance.
This script demonstrates how to use the KoreanStockCollector class.
"""

import sys
import os
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from data_collection.korean_stocks import KoreanStockCollector
import pandas as pd


def test_stock_info():
    """Test getting stock information."""
    print("🔍 Testing stock information retrieval...")
    
    collector = KoreanStockCollector()
    
    # Test Samsung Electronics (KOSPI)
    samsung_info = collector.get_stock_info("005930", "KOSPI")
    if samsung_info:
        print(f"✅ Samsung Electronics: {samsung_info.get('longName', 'Unknown')}")
        print(f"   Market Cap: {samsung_info.get('marketCap', 'N/A'):,}")
        print(f"   Current Price: {samsung_info.get('currentPrice', 'N/A')}")
        print(f"   P/E Ratio: {samsung_info.get('trailingPE', 'N/A')}")
    else:
        print("❌ Failed to get Samsung Electronics info")
    
    # Test NAVER (KOSPI)
    naver_info = collector.get_stock_info("035420", "KOSPI")
    if naver_info:
        print(f"✅ NAVER: {naver_info.get('longName', 'Unknown')}")
        print(f"   Market Cap: {naver_info.get('marketCap', 'N/A'):,}")
        print(f"   Current Price: {naver_info.get('currentPrice', 'N/A')}")
    else:
        print("❌ Failed to get NAVER info")


def test_historical_data():
    """Test getting historical stock data."""
    print("\n📈 Testing historical data retrieval...")
    
    collector = KoreanStockCollector()
    
    # Get Samsung Electronics historical data (last 30 days)
    samsung_data = collector.get_stock_data("005930", "KOSPI", period="1mo")
    
    if not samsung_data.empty:
        print(f"✅ Samsung Electronics historical data: {len(samsung_data)} records")
        print("   Latest data:")
        latest = samsung_data.iloc[-1]
        print(f"   Date: {latest['Date']}")
        print(f"   Close: {latest['Close']}")
        print(f"   Volume: {latest['Volume']:,}")
        print(f"   High: {latest['High']}")
        print(f"   Low: {latest['Low']}")
    else:
        print("❌ Failed to get Samsung Electronics historical data")


def test_market_summary():
    """Test getting market summary."""
    print("\n📊 Testing market summary retrieval...")
    
    collector = KoreanStockCollector()
    
    # Get KOSPI market summary
    kospi_summary = collector.get_market_summary("KOSPI")
    if kospi_summary:
        print(f"✅ KOSPI Market Summary:")
        print(f"   Current Price: {kospi_summary.get('current_price', 'N/A')}")
        print(f"   Change: {kospi_summary.get('change', 'N/A')}")
        print(f"   Change %: {kospi_summary.get('change_percent', 'N/A'):.2f}%")
        print(f"   Volume: {kospi_summary.get('volume', 'N/A'):,}")
    else:
        print("❌ Failed to get KOSPI market summary")
    
    # Get KOSDAQ market summary
    kosdaq_summary = collector.get_market_summary("KOSDAQ")
    if kosdaq_summary:
        print(f"✅ KOSDAQ Market Summary:")
        print(f"   Current Price: {kosdaq_summary.get('current_price', 'N/A')}")
        print(f"   Change: {kosdaq_summary.get('change', 'N/A')}")
        print(f"   Change %: {kosdaq_summary.get('change_percent', 'N/A'):.2f}%")
    else:
        print("❌ Failed to get KOSDAQ market summary")


def test_top_stocks():
    """Test getting top stocks."""
    print("\n🏆 Testing top stocks retrieval...")
    
    collector = KoreanStockCollector()
    
    # Get top 10 KOSPI stocks by market cap
    top_kospi = collector.get_top_stocks("KOSPI", top_n=10)
    
    if not top_kospi.empty:
        print(f"✅ Top 10 KOSPI stocks by market cap:")
        for idx, row in top_kospi.iterrows():
            print(f"   {idx+1}. {row.get('longName', row.get('symbol', 'Unknown'))}")
            print(f"      Symbol: {row.get('symbol', 'N/A')}")
            print(f"      Market Cap: {row.get('marketCap', 'N/A'):,}")
            print(f"      Current Price: {row.get('currentPrice', 'N/A')}")
    else:
        print("❌ Failed to get top KOSPI stocks")


def test_stock_list():
    """Test getting stock list."""
    print("\n📋 Testing stock list retrieval...")
    
    collector = KoreanStockCollector()
    
    # Get KOSPI stock list
    kospi_stocks = collector.get_stock_list("KOSPI")
    
    if not kospi_stocks.empty:
        print(f"✅ KOSPI stock list: {len(kospi_stocks)} stocks")
        print("   Sample stocks:")
        for idx, row in kospi_stocks.head(5).iterrows():
            print(f"   - {row.get('longName', row.get('symbol', 'Unknown'))} ({row.get('symbol', 'N/A')})")
    else:
        print("❌ Failed to get KOSPI stock list")


def test_data_saving():
    """Test saving data to CSV."""
    print("\n💾 Testing data saving...")
    
    collector = KoreanStockCollector()
    
    # Get Samsung Electronics data
    samsung_data = collector.get_stock_data("005930", "KOSPI", period="1mo")
    
    if not samsung_data.empty:
        # Save to CSV
        output_path = "data/raw/korean_stocks/samsung_electronics.csv"
        collector.save_to_csv(samsung_data, output_path)
        print(f"✅ Data saved to {output_path}")
    else:
        print("❌ No data to save")


def main():
    """Run all tests."""
    print("🚀 Testing Korean Stock Market Data Collection")
    print("=" * 50)
    
    try:
        # Run all tests
        test_stock_info()
        test_historical_data()
        test_market_summary()
        test_top_stocks()
        test_stock_list()
        test_data_saving()
        
        print("\n" + "=" * 50)
        print("✅ All tests completed successfully!")
        print("\n📝 Usage examples:")
        print("   from src.data_collection.korean_stocks import KoreanStockCollector")
        print("   collector = KoreanStockCollector()")
        print("   data = collector.get_stock_data('005930', 'KOSPI', period='1mo')")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
