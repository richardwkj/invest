#!/usr/bin/env python3
"""
Script to set up PostgreSQL database for the invest_stocks project.
This script creates the database, user, and sets up basic permissions.
"""

import sys
from pathlib import Path
import os
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def main():
    """Main function to set up PostgreSQL database."""
    print("🐘 PostgreSQL Database Setup Script")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    print()
    
    print("📋 This script will help you set up the PostgreSQL database.")
    print("   Please ensure PostgreSQL is running on your system.")
    print()
    
    print("🔧 Manual Setup Instructions:")
    print("=" * 60)
    print()
    print("1. Connect to PostgreSQL as superuser:")
    print("   psql -U postgres")
    print()
    print("2. Create the database:")
    print("   CREATE DATABASE invest_stocks;")
    print()
    print("3. Create a dedicated user (optional but recommended):")
    print("   CREATE USER invest_user WITH PASSWORD 'your_secure_password';")
    print()
    print("4. Grant privileges:")
    print("   GRANT ALL PRIVILEGES ON DATABASE invest_stocks TO invest_user;")
    print()
    print("5. Connect to the new database:")
    print("   \\c invest_stocks")
    print()
    print("6. Grant schema privileges:")
    print("   GRANT ALL ON SCHEMA public TO invest_user;")
    print()
    print("7. Exit PostgreSQL:")
    print("   \\q")
    print()
    print("🔗 Connection String:")
    print("   postgresql://postgres:Lwhfy!3!a@localhost:5432/invest_stocks")
    print("   postgresql://invest_user:your_secure_password@localhost:5432/invest_stocks")
    print()
    print("⚠️  Security Notes:")
    print("   - Change 'your_secure_password' to a strong password")
    print("   - Consider using environment variables for passwords")
    print("   - Restrict user permissions as needed for production")
    print()
    print("📁 After setup, you can run:")
    print("   python scripts/collect_kr_stock_codes.py")
    print("   python scripts/collect_korean_stocks.py")
    print()
    
    # Check if PostgreSQL is accessible
    try:
        import psycopg2
        print("✅ psycopg2 package is available")
    except ImportError:
        print("❌ psycopg2 package not found!")
        print("   Please install: pip install psycopg2-binary")
        print("   or: uv add psycopg2-binary")
        print()
    
    print("=" * 60)
    print(f"Finished at: {datetime.now()}")

if __name__ == "__main__":
    main()
