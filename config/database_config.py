"""
Database configuration for the invest_stocks project.
This file contains PostgreSQL connection settings and database configuration.
"""


import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from typing import Optional

# Define connection parameters. Connect to the default 'postgres' database.
# Replace with your own details if necessary.
conn_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Lwhfyuauau!#13a",
    "host": "localhost",
    "port": "5432"
}

new_db_name = "invest_stocks"
invest_user_password = "Lwhfy!3!a"  # Password for invest_user

def create_database():
    """Create the invest_stocks database if it doesn't exist."""
    try:
        # 1. Establish a connection to the default database
        conn = psycopg2.connect(**conn_params)

        # 2. Set the isolation level to autocommit
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # 3. Create a cursor object
        cursor = conn.cursor()

        # 4. Check if database already exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (new_db_name,))
        if cursor.fetchone():
            print(f"Database '{new_db_name}' already exists.")
        else:
            # 5. Execute the CREATE DATABASE command
            cursor.execute(f"CREATE DATABASE {new_db_name}")
            print(f"Database '{new_db_name}' created successfully.")

    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # 6. Close the cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def create_user_and_grant_privileges():
    """Create invest_user and grant privileges to invest_stocks database."""
    try:
        # 1. Connect to postgres database to create user
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # 2. Check if user already exists
        cursor.execute("SELECT 1 FROM pg_user WHERE usename = 'invest_user'")
        if cursor.fetchone():
            print("User 'invest_user' already exists.")
            # Update password if user exists
            cursor.execute("ALTER USER invest_user WITH PASSWORD %s", (invest_user_password,))
            print("Password updated for 'invest_user'.")
        else:
            # 3. Create the user
            cursor.execute("CREATE USER invest_user WITH PASSWORD %s", (invest_user_password,))
            print("User 'invest_user' created successfully.")

        # 4. Grant privileges on the database
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {new_db_name} TO invest_user")
        print(f"Granted all privileges on '{new_db_name}' to 'invest_user'.")

        # 5. Connect to invest_stocks database to grant schema privileges
        cursor.close()
        conn.close()

        # Connect to invest_stocks database
        invest_conn_params = conn_params.copy()
        invest_conn_params["dbname"] = new_db_name
        invest_conn = psycopg2.connect(**invest_conn_params)
        invest_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        invest_cursor = invest_conn.cursor()

        # 6. Grant schema privileges
        invest_cursor.execute("GRANT ALL ON SCHEMA public TO invest_user")
        print("Granted schema privileges to 'invest_user'.")

        # 7. Grant future table privileges
        invest_cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO invest_user")
        print("Granted future table privileges to 'invest_user'.")

        invest_cursor.close()
        invest_conn.close()

        print("✅ User setup completed successfully!")

    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_database_url(user_type: str = "default") -> str:
    """Get the PostgreSQL database connection URL for different users."""
    if user_type == "invest_user":
        return f"postgresql://invest_user:{invest_user_password}@localhost:5432/{new_db_name}"
    elif user_type == "postgres":
        return f"postgresql://postgres:Lwhfyuauau!#13a@localhost:5432/{new_db_name}"
    else:
        # Default to invest_user for better security
        return f"postgresql://invest_user:{invest_user_password}@localhost:5432/{new_db_name}"

def test_connection(user_type: str = "default"):
    """Test connection to the database with specified user."""
    try:
        url = get_database_url(user_type)
        print(f"Testing connection for {user_type}...")
        print(f"URL: {url.replace('Lwhfy!3!a', '***').replace('Lwhfyuauau!#13a', '***')}")
        
        conn = psycopg2.connect(url)
        cursor = conn.cursor()
        cursor.execute("SELECT current_user, current_database(), version()")
        user, db, version = cursor.fetchone()
        
        print(f"✅ Connection successful!")
        print(f"   User: {user}")
        print(f"   Database: {db}")
        print(f"   Version: {version}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    print("🐘 PostgreSQL Database and User Setup")
    print("=" * 50)
    
    # Create database
    print("\n1️⃣ Creating database...")
    create_database()
    
    # Create user and grant privileges
    print("\n2️⃣ Creating user and granting privileges...")
    create_user_and_grant_privileges()
    
    # Test connections
    print("\n3️⃣ Testing connections...")
    print("\nTesting postgres user connection:")
    test_connection("postgres")
    
    print("\nTesting invest_user connection:")
    test_connection("invest_user")
    
    print("\n" + "=" * 50)
    print("🎯 Setup complete! You can now use either user to connect:")
    print(f"   - postgres: postgresql://postgres:***@localhost:5432/{new_db_name}")
    print(f"   - invest_user: postgresql://invest_user:***@localhost:5432/{new_db_name}")



