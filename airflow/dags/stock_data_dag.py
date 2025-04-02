from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adjust this path to point to your scripts directory
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
scripts_dir = os.path.join(project_dir, 'scripts')
sys.path.append(scripts_dir)

# Import your scripts
from extract_stock_data import get_daily_stock_data
from load_to_sqlite import create_sqlite_db, load_to_sqlite
import sqlite3
import pandas as pd
from datetime import datetime

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# API configuration
SYMBOLS = ["MSFT", "AAPL", "GOOGL", "AMZN", "META"]

# SQLite configuration
SQLITE_DB_PATH = os.path.join(project_dir, 'data', 'stock_data.db')
SQLITE_TABLE = "daily_stock_prices"

def extract_stock_data(**kwargs):
    """Extract daily stock data for all symbols"""
    all_data = pd.DataFrame()
    
    for symbol in SYMBOLS:
        print(f"Fetching data for {symbol}...")
        df = get_daily_stock_data(symbol)
        
        if df is not None:
            all_data = pd.concat([all_data, df])
    
    # Save to temporary CSV file
    if not all_data.empty:
        # Create data directory if it doesn't exist
        data_dir = os.path.join(project_dir, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # Current date for filename
        today = datetime.now().strftime("%Y%m%d")
        csv_path = os.path.join(data_dir, f"stock_data_{today}.csv")
        all_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
        
        # Push the DataFrame to XCom as a serialized CSV string
        kwargs['ti'].xcom_push(key='stock_data_path', value=csv_path)
        return csv_path
    else:
        print("No data was collected!")
        return None

def load_to_sqlite_db(**kwargs):
    """Load data to SQLite"""
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='extract_stock_data', key='stock_data_path')
    
    if not csv_path or not os.path.exists(csv_path):
        print("No data file available for loading")
        return
    
    # Read data from CSV
    df = pd.read_csv(csv_path)
    
    # Create SQLite database and table
    create_sqlite_db()
    
    # Load data to SQLite
    load_to_sqlite(df)
    
    return f"Data loaded to SQLite from {csv_path}"

def execute_sql_transformations(**kwargs):
    """Run SQL transformations"""
    # Read SQL file
    sql_file = os.path.join(scripts_dir, 'sqlite_transformations.sql')
    
    with open(sql_file, 'r') as f:
        sql_script = f.read()
    
    # Split into individual statements
    statements = sql_script.split(';')
    
    # Connect to database
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    
    # Execute each statement
    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
                print(f"Executed: {statement[:50]}...")  # Print first 50 chars
            except sqlite3.Error as e:
                print(f"Error executing: {statement[:50]}...\nError: {e}")
    
    conn.commit()
    conn.close()
    
    return "SQL transformations completed"

# Create DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Pipeline to fetch and load daily stock data',
    schedule_interval='0 18 * * 1-5',  # Run at 6 PM on weekdays (after market close)
    catchup=False
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_stock_data',
    python_callable=extract_stock_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_sqlite',
    python_callable=load_to_sqlite_db,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='execute_sql_transformations',
    python_callable=execute_sql_transformations,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> load_task >> transform_task