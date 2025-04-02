import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API configuration
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = ["MSFT", "AAPL", "GOOGL", "AMZN", "META"]

def get_daily_stock_data(symbol):
    """Fetch daily stock data for a given symbol"""
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }
    
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    # Check if API call was successful
    if "Error Message" in data:
        print(f"Error fetching data for {symbol}: {data['Error Message']}")
        return None
    
    # Extract time series data
    time_series = data.get("Time Series (Daily)", {})
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(time_series, orient="index")
    
    # Rename columns
    df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    }, inplace=True)
    
    # Add symbol column
    df["symbol"] = symbol
    
    # Convert string values to numeric
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col])
    
    # Reset index to make date a column and name it properly
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    
    return df

def main():
    # Create output directory if it doesn't exist
    output_dir = "../data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Current date for filename
    today = datetime.now().strftime("%Y%m%d")
    
    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()
    
    # Collect data for each symbol
    for symbol in SYMBOLS:
        print(f"Fetching data for {symbol}...")
        df = get_daily_stock_data(symbol)
        
        if df is not None:
            all_data = pd.concat([all_data, df])
    
    # Save to CSV
    if not all_data.empty:
        csv_path = f"{output_dir}/stock_data_{today}.csv"
        all_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data was collected!")

if __name__ == "__main__":
    main()