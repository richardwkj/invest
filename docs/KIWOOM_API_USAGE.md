# Kiwoom API Integration Guide

## Overview

This guide explains how to use the Kiwoom API integration scripts to collect Korean stock market data. The scripts use the Kiwoom test server for development and testing purposes.

## Files Created

### 1. `scripts/collect_korean_stocks_kiwoom.py`
Main script for collecting Korean stock data using the Kiwoom API.

### 2. `scripts/test_kiwoom_api.py`
Simple test script to verify Kiwoom API connection and authentication.

### 3. `config/kiwoom_config.py`
Configuration file containing Kiwoom API settings and credentials.

## Prerequisites

1. **Python Dependencies**: Ensure you have the required packages installed:
   ```bash
   pip install requests pandas
   ```

2. **Virtual Environment**: Activate your virtual environment:
   ```bash
   # Windows
   .venv\Scripts\activate
   
   # Linux/Mac
   source .venv/bin/activate
   ```

3. **Database Setup**: Ensure your Korean stock codes database is set up and populated.

## Configuration

### Environment Variables (Optional)
You can set these environment variables to override the defaults:

```bash
export KIWOOM_APP_KEY="your_app_key_here"
export KIWOOM_SECRET_KEY="your_secret_key_here"
export KIWOOM_USE_TEST_SERVER="true"
```

### Default Configuration
The scripts use these default values from `config/kiwoom_config.py`:
- **App Key**: `U3ThWWKkTyEMSP-XX8l80bp2ulbc8HVIOWyJvAyGAHA`
- **Secret Key**: `yQWeiWlkEQmyE_QEHB0zqW7Mbc2SPt6IuAE1yiyZQvQ`
- **Test Server**: `true` (uses `https://mockapi.kiwoom.com`)

## Usage

### 1. Test API Connection

First, test the Kiwoom API connection:

```bash
python scripts/test_kiwoom_api.py
```

This script will:
- Test authentication with the Kiwoom API
- Verify access token generation
- Test a simple API call for Samsung Electronics stock data

**Expected Output**:
```
🧪 Testing Kiwoom API Authentication
==================================================
🔧 Configuration:
   Host: https://mockapi.kiwoom.com
   App Key: U3ThWWKkTyEMSP-XX8l80b...
   Secret Key: yQWeiWlkEQmyE_QEHB0z...
   Test Server: True

🔑 Step 1: Requesting access token...
   Status Code: 200
   Response Headers: {...}
   Response Body: {...}

✅ Authentication successful!
   Access Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

🔍 Step 2: Testing API call with access token...
   Testing API call for Samsung Electronics (005930)...
   API Call Status: 200
   API Response: {...}

✅ API call successful!
   Retrieved 10 records
   Sample data: Date=20241201, Close=+36600
```

### 2. Collect Stock Data

Once the API connection is verified, collect stock data:

```bash
python scripts/collect_korean_stocks_kiwoom.py
```

This script will:
- Initialize the Kiwoom stock collector
- Get stock codes from your existing database
- Collect historical price data for the first 5 stocks (configurable)
- Save data to CSV files in `data/raw/korean_stocks/kiwoom/`

**Expected Output**:
```
🇰🇷 Kiwoom Korean Stock Data Collection Script
============================================================
Started at: 2024-12-08 20:30:00

🔧 Configuration:
   App Key: U3ThWWKkTyEMSP-XX8l80b...
   Secret Key: yQWeiWlkEQmyE_QEHB0z...
   Test Server: True

🔧 Initializing Kiwoom Stock Collector...
🧪 Using Kiwoom TEST server
✅ Collector initialized successfully

📋 Getting stock codes from database...
✅ Retrieved 5 stock codes for testing
   Test stocks: 005930, 000660, 035420, 051910, 006400

📅 Date range: 20241108 to 20241208

🚀 Starting data collection...
   This will take a while due to rate limiting.
   Estimated time: ~0.1 minutes for 5 stocks

📊 Processing 1/5: 005930
✅ Successfully collected data for 005930
⏳ Waiting 1.0 seconds before next request...

📊 Processing 2/5: 000660
✅ Successfully collected data for 000660
⏳ Waiting 1.0 seconds before next request...

...

🎯 Collection completed. Successfully collected data for 5 stocks

✅ Data collection completed successfully!

📊 Data Collection Summary:
   Total stocks processed: 5
   Total records: 150
   Date range: 2024-11-08 to 2024-12-08
   Output directory: data/raw/korean_stocks/kiwoom
```

## Data Output

### CSV Files Generated

1. **Individual Stock Files**: `{stock_code}_kiwoom_data.csv`
   - Contains data for a single stock
   - Includes all available fields from the API

2. **Combined Data File**: `combined_kiwoom_data_{timestamp}.csv`
   - Contains data for all stocks in a single file
   - Useful for analysis and processing

### Data Fields

The collected data includes these standardized fields:

| Field | Description | Original API Field |
|-------|-------------|-------------------|
| `stock_code` | Stock code (e.g., '005930') | Added by script |
| `date` | Trading date (YYYY-MM-DD) | `date` (converted) |
| `open_price` | Opening price | `open_pric` |
| `high_price` | Highest price | `high_pric` |
| `low_price` | Lowest price | `low_pric` |
| `close_price` | Closing price | `close_pric` |
| `price_change` | Price change from previous day | `pred_rt` |
| `fluctuation_rate` | Price fluctuation rate (%) | `flu_rt` |
| `volume` | Trading volume | `trde_qty` |
| `amount` | Trading amount (millions) | `amt_mn` |
| `credit_rate` | Credit ratio | `crd_rt` |
| `foreign_rate` | Foreign ownership rate | `for_rt` |
| `foreign_possession` | Foreign possession | `for_poss` |
| `foreign_weight` | Foreign weight | `for_wght` |

## Rate Limiting

The scripts implement rate limiting to respect API limits:
- **Default Delay**: 1 second between API calls
- **Configurable**: Modify `KIWOOM_DEFAULT_DELAY` in `config/kiwoom_config.py`
- **Estimated Time**: For 100 stocks with 1-second delay = ~1.7 minutes

## Error Handling

The scripts include comprehensive error handling:
- **Authentication Errors**: Clear messages for API key issues
- **Network Errors**: Timeout and connection error handling
- **Data Errors**: Graceful handling of malformed responses
- **Rate Limiting**: Automatic delays between requests

## Customization

### Modify Stock Selection

To change which stocks are collected, modify this section in `collect_korean_stocks_kiwoom.py`:

```python
# Get first 5 stocks for testing (you can modify this)
test_stock_codes = stock_codes_df['stock_code'].head(5).tolist()

# Or collect all stocks (be careful with rate limiting)
# test_stock_codes = stock_codes_df['stock_code'].tolist()

# Or specify specific stocks
# test_stock_codes = ['005930', '000660', '035420']  # Samsung, SK Hynix, NAVER
```

### Modify Date Range

To change the date range, modify this section:

```python
# Set date range (last 30 days for testing)
end_date = datetime.now()
start_date = end_date - timedelta(days=30)  # Change 30 to desired days

# Or specify exact dates
# start_date = datetime(2024, 1, 1)
# end_date = datetime(2024, 12, 31)
```

### Switch to Production Server

To use the production server instead of the test server:

```bash
export KIWOOM_USE_TEST_SERVER="false"
```

Or modify `config/kiwoom_config.py`:
```python
KIWOOM_USE_TEST_SERVER = False
```

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify your API keys are correct
   - Check if the test server is accessible
   - Ensure your Kiwoom account is active

2. **No Data Returned**
   - Verify stock codes exist in the Korean market
   - Check date range validity
   - Ensure the stock was active during the specified period

3. **Rate Limiting Errors**
   - Increase delay between API calls
   - Reduce the number of stocks processed at once
   - Check Kiwoom API documentation for current limits

4. **Import Errors**
   - Ensure virtual environment is activated
   - Verify all dependencies are installed
   - Check Python path configuration

### Debug Mode

Enable debug logging by modifying the logger configuration in `src/utils/logger.py`:

```python
# Set log level to DEBUG
logger.add(sys.stderr, level="DEBUG")
```

## Next Steps

1. **Test with a few stocks** to verify everything works
2. **Expand to more stocks** once testing is successful
3. **Integrate with your database** to store the collected data
4. **Set up automated collection** for regular data updates
5. **Add data validation** and quality checks

## Security Notes

- **API Keys**: Never commit API keys to version control
- **Test Server**: Always test with the test server first
- **Rate Limiting**: Respect API limits to avoid account suspension
- **Data Validation**: Validate all collected data before analysis

## Support

For issues with the Kiwoom API:
- Check the [Kiwoom API documentation](https://www.kiwoom.com/)
- Verify your account status and API access
- Contact Kiwoom support for technical issues

For issues with these scripts:
- Check the logs in `logs/app.log`
- Verify your configuration in `config/kiwoom_config.py`
- Ensure all dependencies are properly installed
