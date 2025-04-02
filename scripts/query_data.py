import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

def connect_to_db():
    """Connect to SQLite database"""
    db_path = "../data/stock_data.db"
    return sqlite3.connect(db_path)

def run_query(query):
    """Run SQL query and return results as DataFrame"""
    conn = connect_to_db()
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result

def list_tables():
    """List all tables in the database"""
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' OR type='view'")
    tables = cursor.fetchall()
    conn.close()
    
    print("Tables and views in the database:")
    for table in tables:
        print(f"- {table[0]}")

def plot_stock_prices():
    """Plot closing prices for all stocks"""
    query = """
    SELECT date, symbol, close
    FROM daily_stock_prices
    ORDER BY date
    """
    
    data = run_query(query)
    
    # Convert date to datetime
    data['date'] = pd.to_datetime(data['date'])
    
    # Create plot directory
    plots_dir = "../data/plots"
    os.makedirs(plots_dir, exist_ok=True)
    
    # Plot data
    plt.figure(figsize=(12, 6))
    
    for symbol in data['symbol'].unique():
        symbol_data = data[data['symbol'] == symbol]
        plt.plot(symbol_data['date'], symbol_data['close'], label=symbol)
    
    plt.title('Stock Closing Prices')
    plt.xlabel('Date')
    plt.ylabel('Price ($)')
    plt.legend()
    plt.grid(True)
    
    # Save plot
    plt.savefig(f"{plots_dir}/stock_prices.png")
    print(f"Plot saved to {plots_dir}/stock_prices.png")
    
    # Show plot
    plt.show()

def plot_daily_changes():
    """Plot daily percentage changes"""
    query = """
    SELECT date, symbol, daily_change_pct
    FROM stock_performance
    ORDER BY date
    """
    
    data = run_query(query)
    
    # Convert date to datetime
    data['date'] = pd.to_datetime(data['date'])
    
    # Create plot directory
    plots_dir = "../data/plots"
    os.makedirs(plots_dir, exist_ok=True)
    
    # Plot data
    plt.figure(figsize=(12, 6))
    
    for symbol in data['symbol'].unique():
        symbol_data = data[data['symbol'] == symbol]
        plt.plot(symbol_data['date'], symbol_data['daily_change_pct'], label=symbol)
    
    plt.title('Daily Stock Price Changes (%)')
    plt.xlabel('Date')
    plt.ylabel('Change (%)')
    plt.legend()
    plt.grid(True)
    
    # Save plot
    plt.savefig(f"{plots_dir}/daily_changes.png")
    print(f"Plot saved to {plots_dir}/daily_changes.png")
    
    # Show plot
    plt.show()

def main():
    """Main function to demonstrate database queries"""
    print("Stock Data Query Tool")
    print("--------------------")
    
    # List tables
    list_tables()
    
    print("\nOptions:")
    print("1. Plot stock prices")
    print("2. Plot daily changes")
    print("3. Run custom query")
    
    choice = input("\nEnter your choice (1-3): ")
    
    if choice == '1':
        plot_stock_prices()
    elif choice == '2':
        plot_daily_changes()
    elif choice == '3':
        query = input("Enter your SQL query: ")
        result = run_query(query)
        print("\nQuery result:")
        print(result)
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()