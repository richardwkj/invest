#!/usr/bin/env python3
"""
Simple test script to verify Kiwoom API connection.
This script tests the basic authentication and API response.
"""

import sys
import os
import requests
import json
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from config.kiwoom_config import (
    get_kiwoom_host, 
    get_kiwoom_credentials, 
    is_test_server
)

def test_kiwoom_authentication():
    """Test Kiwoom API authentication."""
    print("🧪 Testing Kiwoom API Authentication")
    print("=" * 50)
    
    # Get configuration
    host = get_kiwoom_host()
    app_key, secret_key = get_kiwoom_credentials()
    use_test_server = is_test_server()
    
    print(f"🔧 Configuration:")
    print(f"   Host: {host}")
    print(f"   App Key: {app_key[:20]}...")
    print(f"   Secret Key: {secret_key[:20]}...")
    print(f"   Test Server: {use_test_server}")
    print()
    
    try:
        # 1. Request access token
        endpoint = '/oauth2/token'
        url = host + endpoint
        
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
        }
        
        data = {
            'grant_type': 'client_credentials',
            'appkey': app_key,
            'secretkey': secret_key,
        }
        
        print("🔑 Step 1: Requesting access token...")
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        print(f"   Status Code: {response.status_code}")
        print(f"   Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            response_data = response.json()
            print(f"   Response Body: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
            
            if response_data.get('return_code') == 0:
                access_token = response_data.get('token')  # Kiwoom uses 'token' not 'access_token'
                print(f"\n✅ Authentication successful!")
                print(f"   Access Token: {access_token[:50]}..." if access_token else "   No access token in response")
                
                # Test if we can use the token for a simple API call
                if access_token:
                    print(f"\n🔍 Step 2: Testing API call with access token...")
                    test_api_call(host, access_token, app_key, secret_key)
                else:
                    print(f"\n⚠️ No access token received - cannot test API calls")
                    
            else:
                print(f"\n❌ Authentication failed:")
                print(f"   Return Code: {response_data.get('return_code')}")
                print(f"   Return Message: {response_data.get('return_msg')}")
                
        else:
            print(f"\n❌ HTTP error {response.status_code}:")
            print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"\n❌ Error during authentication test: {e}")
        import traceback
        traceback.print_exc()

def test_api_call(host: str, access_token: str, app_key: str, secret_key: str):
    """Test a simple API call using the access token."""
    try:
        # Test with Samsung Electronics (005930) - KOSPI stock
        # Based on the sample response, this might be a different endpoint
        endpoint = '/uapi/domestic-stock/v1/quotations/inquire-price'
        url = host + endpoint
        
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'authorization': f'Bearer {access_token}',
            'appkey': app_key,
            'appsecret': secret_key,
            'tr_id': 'FHKST01010100',
        }
        
        # Try with simpler parameters first
        params = {
            'FID_COND_MRKT_DIV_CODE': 'J',  # KOSPI
            'FID_COND_SCR_DIV_CODE': '20171',  # Stock
            'FID_INPUT_ISCD': '005930',  # Samsung Electronics
            'FID_INPUT_DATE_1': '20241201',  # Start date
            'FID_INPUT_DATE_2': '20241208',  # End date
            'FID_VOL_CNT': '10',  # Number of records
        }
        
        print(f"   Testing with endpoint: {endpoint}")
        print(f"   Parameters: {params}")
        
        print(f"   Testing API call for Samsung Electronics (005930)...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        print(f"   API Call Status: {response.status_code}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"   API Response: {json.dumps(api_response, indent=2, ensure_ascii=False)}")
            
            if api_response.get('return_code') == 0:
                print(f"\n✅ API call successful!")
                stock_data = api_response.get('daly_stkpc', [])
                if stock_data:
                    print(f"   Retrieved {len(stock_data)} records")
                    # Show first record as sample
                    if stock_data:
                        first_record = stock_data[0]
                        print(f"   Sample data: Date={first_record.get('date')}, Close={first_record.get('close_pric')}")
                else:
                    print(f"   No stock data returned")
            else:
                print(f"\n❌ API call failed:")
                print(f"   Return Code: {api_response.get('return_code')}")
                print(f"   Return Message: {api_response.get('return_msg')}")
        else:
            print(f"\n❌ API call HTTP error {response.status_code}:")
            print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"\n❌ Error during API call test: {e}")

def main():
    """Main function to run the Kiwoom API test."""
    print("🇰🇷 Kiwoom API Connection Test")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Test authentication
    test_kiwoom_authentication()
    
    print(f"\nFinished at: {datetime.now()}")
    print("=" * 60)

if __name__ == "__main__":
    from datetime import datetime
    main()
