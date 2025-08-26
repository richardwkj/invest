"""
Kiwoom API configuration for the Invest Analytics application.
This file contains Kiwoom API credentials and configuration settings.
"""

import os
from typing import Optional

# Kiwoom API Configuration
KIWOOM_APP_KEY = os.getenv('KIWOOM_APP_KEY', 'U3ThWWKkTyEMSP-XX8l80bp2ulbc8HVIOWyJvAyGAHA')
KIWOOM_SECRET_KEY = os.getenv('KIWOOM_SECRET_KEY', 'yQWeiWlkEQmyE_QEHB0zqW7Mbc2SPt6IuAE1yiyZQvQ')
KIWOOM_USE_TEST_SERVER = os.getenv('KIWOOM_USE_TEST_SERVER', 'true').lower() == 'true'

# API Endpoints
KIWOOM_TEST_HOST = 'https://mockapi.kiwoom.com'
KIWOOM_PROD_HOST = 'https://api.kiwoom.com'

# Rate Limiting
KIWOOM_DEFAULT_DELAY = 1.0  # seconds between API calls
KIWOOM_MAX_RETRIES = 3

# Data Collection Settings
KIWOOM_DEFAULT_DATE_RANGE_DAYS = 30
KIWOOM_MAX_RECORDS_PER_REQUEST = 100

# Stock Market Codes
KIWOOM_MARKET_CODES = {
    'KOSPI': 'J',
    'KOSDAQ': 'K',
    'KONEX': 'N'
}

# Transaction IDs
KIWOOM_TR_IDS = {
    'HISTORICAL_PRICE': 'FHKST01010100',
    'REAL_TIME_PRICE': 'FHKST01010100',
    'DAILY_PRICE': 'FHKST01010100'
}

def get_kiwoom_host() -> str:
    """Get the appropriate Kiwoom host URL based on configuration."""
    return KIWOOM_TEST_HOST if KIWOOM_USE_TEST_SERVER else KIWOOM_PROD_HOST

def get_kiwoom_credentials() -> tuple:
    """Get Kiwoom API credentials."""
    return KIWOOM_APP_KEY, KIWOOM_SECRET_KEY

def is_test_server() -> bool:
    """Check if using test server."""
    return KIWOOM_USE_TEST_SERVER

def get_rate_limit_delay() -> float:
    """Get the rate limiting delay between API calls."""
    return KIWOOM_DEFAULT_DELAY

def get_max_retries() -> int:
    """Get maximum number of retries for API calls."""
    return KIWOOM_MAX_RETRIES
