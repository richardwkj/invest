#!/usr/bin/env python3
"""
Simple database connection test.
"""

try:
    from sqlalchemy import create_engine, text
    
    # Test direct connection
    print("Testing PostgreSQL connection...")
    url = "postgresql://postgres:Lwhfy!3!a@localhost:5432/invest_stocks"
    print(f"URL: {url.replace('Lwhfy!3!a', '***')}")
    
    engine = create_engine(url)
    
    with engine.connect() as connection:
        result = connection.execute(text("SELECT version();"))
        version = result.fetchone()[0]
        print(f"✅ Success! PostgreSQL version: {version}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"Error type: {type(e).__name__}")
