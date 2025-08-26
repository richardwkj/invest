#!/usr/bin/env python3
"""
Simple test script to replicate the working Kiwoom API call from the sample code.
"""

import requests
import json

def test_kiwoom_simple():
    """Test the exact API call from the sample code."""
    print("🧪 Testing Kiwoom API with Simple Approach")
    print("=" * 50)
    
    # Use the exact credentials and approach from the sample
    app_key = 'U3ThWWKkTyEMSP-XX8l80bp2ulbc8HVIOWyJvAyGAHA'
    secret_key = 'yQWeiWlkEQmyE_QEHB0zqW7Mbc2SPt6IuAE1yiyZQvQ'
    host = 'https://mockapi.kiwoom.com'  # Test server
    
    print(f"🔧 Configuration:")
    print(f"   Host: {host}")
    print(f"   App Key: {app_key[:20]}...")
    print(f"   Secret Key: {secret_key[:20]}...")
    print()
    
    try:
        # Step 1: Get access token (exactly as in sample)
        print("🔑 Step 1: Getting access token...")
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
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            response_data = response.json()
            print(f"   Response: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
            
            if response_data.get('return_code') == 0:
                access_token = response_data.get('token')
                print(f"\n✅ Access token obtained: {access_token[:50]}...")
                
                # Step 2: Try to get stock data
                print(f"\n🔍 Step 2: Trying to get stock data...")
                
                # Try different approaches to see what works
                test_endpoints = [
                    '/uapi/domestic-stock/v1/quotations/inquire-price',
                    '/uapi/domestic-stock/v1/quotations/inquire-daily-price',
                    '/uapi/domestic-stock/v1/quotations/inquire-price-history'
                ]
                
                for endpoint in test_endpoints:
                    print(f"\n   Trying endpoint: {endpoint}")
                    url = host + endpoint
                    
                    headers = {
                        'Content-Type': 'application/json;charset=UTF-8',
                        'authorization': f'Bearer {access_token}',
                        'appkey': app_key,
                        'appsecret': secret_key,
                    }
                    
                    # Try with minimal parameters
                    params = {
                        'FID_INPUT_ISCD': '005930',  # Samsung Electronics
                    }
                    
                    try:
                        response = requests.get(url, headers=headers, params=params, timeout=30)
                        print(f"      Status: {response.status_code}")
                        
                        if response.status_code == 200:
                            data = response.json()
                            print(f"      Success! Response keys: {list(data.keys())}")
                            if 'daly_stkpc' in data:
                                print(f"      Found 'daly_stkpc' data with {len(data['daly_stkpc'])} records")
                        else:
                            print(f"      Error: {response.text[:200]}")
                            
                    except Exception as e:
                        print(f"      Exception: {e}")
                
                # Step 3: Try the exact approach from your sample
                print(f"\n🔍 Step 3: Trying exact sample approach...")
                
                # Based on your sample, it seems like the API might return data directly
                # Let me try to understand what endpoint was used
                print(f"   Your sample shows data with 'daly_stkpc' field")
                print(f"   This suggests a successful API call was made")
                print(f"   Let me check if there are other endpoints...")
                
            else:
                print(f"❌ Failed to get access token: {response_data.get('return_msg')}")
                
        else:
            print(f"❌ HTTP error {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_kiwoom_simple()
