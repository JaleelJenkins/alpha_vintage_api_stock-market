import os
import pandas as pd
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from extract_stock_data import get_daily_stock_data

# Load environment variables
load_dotenv()

# SQLite configuration
SQLITE_DB_PATH = "../data/stock_data.db"
SQLITE_TABLE = "daily_stock_prices"

# API configuration
SYMBOLS = ["MSFT", "AAPL", "GOOGL", "AMZN", "META"]

def create_sqlite_db():
    """Create SQLite database and table if they don't exist"""
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SQLITE_TABLE} (
        date TEXT,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        batch_id TEXT,
        PRIMARY KEY (date, symbol, batch_id)
    )
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()

def load_to_sqlite(df):
    """Load DataFrame to SQLite"""
    # Add batch ID column (using current timestamp)
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df["batch_id"] = batch_id
    
    # Connect to SQLite
    conn = sqlite3.connect(SQLITE_DB_PATH)
    
    # Write the DataFrame to SQLite
    df.to_sql(SQLITE_TABLE, conn, if_exists='append', index=False)
    
    # Count rows inserted
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {SQLITE_TABLE} WHERE batch_id = ?", (batch_id,))
    row_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"Successfully loaded {row_count} rows to SQLite")

def main():
    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()
    
    # Collect data for each symbol
    for symbol in SYMBOLS:
        print(f"Fetching data for {symbol}...")
        df = get_daily_stock_data(symbol)
        
        if df is not None:
            all_data = pd.concat([all_data, df])
    
    # Save to CSV and SQLite
    if not all_data.empty:
        # Save to CSV
        output_dir = "../data"
        os.makedirs(output_dir, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        csv_path = f"{output_dir}/stock_data_{today}.csv"
        all_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
        
        # Create SQLite DB and table
        create_sqlite_db()
        
        # Load to SQLite
        load_to_sqlite(all_data)
    else:
        print("No data was collected!")

if __name__ == "__main__":
    main()