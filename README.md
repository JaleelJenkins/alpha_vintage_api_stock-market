# Stock Data Engineering Pipeline

This project implements a data engineering pipeline that:
1. Extracts stock data from Alpha Vantage API
2. Loads data to Snowflake
3. Transforms data with SQL
4. (Future) Orchestrates the pipeline with Airflow
5. (Future) Visualizes data with Tableau

## Setup

1. Install dependencies:
    pip install requests pandas python-dotenv matplotlib

2. Update the `.env` file with your Alpha Vantage API key:

    Get a free API key from: https://www.alphavantage.co/support/#api-key

3. Run the pipeline:
    cd scripts
    python run_pipeline.py

4. Query and visualize the data:
    cd scripts
    python query_data.py

## Project Structure

- `scripts/` - Python scripts for data extraction and loading
- `extract_stock_data.py` - Extracts data from Alpha Vantage
- `load_to_sqlite.py` - Loads data to SQLite
- `sqlite_transformations.sql` - SQL transformations
- `execute_sql.py` - Executes SQL transformations
- `run_pipeline.py` - Main pipeline script
- `query_data.py` - Tool for querying and visualizing data
- `data/` - Local storage for CSV files and SQLite database
- `logs/` - Pipeline logs

## SQL Transformations

The project automatically applies the following transformations:
- `daily_avg_prices` - Daily average prices for each stock
- `stock_performance` - Daily price changes and performance metrics
- `stock_volatility` - Monthly volatility metrics

## Next Steps

1. Set up Airflow for orchestration
2. Connect to more advanced visualization tools
3. Add more data sources and analysis

## Airflow Setup

To set up Airflow for scheduling the ETL pipeline:

1. Install Airflow:
    pip install apache-airflow==2.7.1

2. Run the setup script:
    ./setup_airflow.sh

3. Start Airflow webserver (in one terminal):
    export AIRFLOW_HOME="$(pwd)/airflow"
    airflow webserver --port 8080

4. Start Airflow scheduler (in another terminal):
    export AIRFLOW_HOME="$(pwd)/airflow"
    airflow scheduler

5. Access the Airflow UI at http://localhost:8080
- Username: admin
- Password: admin

6. The DAG is configured to run at 6 PM on weekdays (after market close)
- You can also trigger it manually from the Airflow UI