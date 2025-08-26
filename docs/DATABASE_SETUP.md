# PostgreSQL Database Setup Guide

This guide will help you set up the PostgreSQL database for the `invest_stocks` project.

## Prerequisites

- PostgreSQL installed and running on your system
- Python with required packages (`psycopg2-binary`, `sqlalchemy`)
- Access to create databases and users (superuser privileges)

## Quick Setup

### 1. Install Required Packages

```bash
# Using uv (recommended)
uv add psycopg2-binary sqlalchemy

# Using pip
pip install psycopg2-binary sqlalchemy
```

### 2. Run the Setup Script

```bash
python scripts/setup_postgresql.py
```

This will provide step-by-step instructions for manual setup.

### 3. Manual Database Creation

Connect to PostgreSQL as superuser:

```bash
psql -U postgres
```

Create the database:

```sql
CREATE DATABASE invest_stocks;
```

Create a dedicated user (recommended):

```sql
CREATE USER invest_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE invest_stocks TO invest_user;
```

Connect to the new database:

```sql
\c invest_stocks
```

Grant schema privileges:

```sql
GRANT ALL ON SCHEMA public TO invest_user;
```

Exit PostgreSQL:

```sql
\q
```

## Configuration

### Environment Variables

Create a `.env` file in your project root:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=invest_stocks
DB_USER=postgres
DB_PASSWORD=Lwhfy!3!a

# Alternative: Use dedicated user
# DB_USER=invest_user
# DB_PASSWORD=your_secure_password

# Connection Pool Settings
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
DB_POOL_RECYCLE=3600
DB_ECHO=False
```

### Connection Strings

The scripts will automatically use the configuration from `config/database_config.py`, which supports:

- **Default**: Uses environment variables
- **Fallback**: Hardcoded connection string
- **Multiple users**: Support for different user accounts

## Running the Scripts

After database setup, you can run:

### 1. Stock Codes Collection

```bash
python scripts/collect_kr_stock_codes.py
```

This will:
- Create the `kr_stock_codes` table
- Collect all Korean stock symbols from pykrx
- Store company names, market classification, and metadata

### 2. Historical Data Collection

```bash
python scripts/collect_korean_stocks.py
```

This will:
- Create tables for historical price data
- Collect OHLCV data for all Korean stocks
- Store time series data in the database

## Database Schema

### kr_stock_codes Table

```sql
CREATE TABLE kr_stock_codes (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) UNIQUE NOT NULL,
    stock_market VARCHAR(10) NOT NULL,  -- KOSPI or KOSDAQ
    company_name VARCHAR(200),
    ipo_date DATE,
    delisting_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATE DEFAULT CURRENT_DATE,
    updated_at DATE DEFAULT CURRENT_DATE
);
```

### Indexes

The following indexes are automatically created:

```sql
-- Market and active status
CREATE INDEX idx_kr_stock_codes_market_active 
ON kr_stock_codes(stock_market, is_active);

-- IPO dates
CREATE INDEX idx_kr_stock_codes_ipo_date 
ON kr_stock_codes(ipo_date);

-- Delisting dates
CREATE INDEX idx_kr_stock_codes_delisting_date 
ON kr_stock_codes(delisting_date);
```

## Troubleshooting

### Connection Issues

1. **Database doesn't exist**: Run the setup script or create manually
2. **Authentication failed**: Check username/password in configuration
3. **Connection refused**: Ensure PostgreSQL is running on the specified port

### Permission Issues

1. **Insufficient privileges**: Grant necessary permissions to your user
2. **Schema access denied**: Grant access to the public schema

### Performance Issues

1. **Slow queries**: Check if indexes are created properly
2. **Connection pool exhausted**: Adjust pool size in configuration

## Security Considerations

- Use strong passwords for database users
- Consider using environment variables for sensitive data
- Restrict user permissions based on your security requirements
- Use dedicated users instead of superuser accounts for application connections

## Next Steps

After successful database setup:

1. Run `collect_kr_stock_codes.py` to populate stock metadata
2. Run `collect_korean_stocks.py` to collect historical price data
3. Verify data integrity and performance
4. Set up regular data collection schedules

## Support

If you encounter issues:

1. Check the PostgreSQL logs for error messages
2. Verify connection parameters and permissions
3. Ensure all required packages are installed
4. Check the script logs for detailed error information
