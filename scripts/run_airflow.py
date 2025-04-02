"""
Helper script to run Airflow DAG manually for testing.
This is an alternative to the Airflow UI for quick testing.
"""

import os
import sys
import subprocess
from datetime import datetime

# Adjust path to include project root
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_dir)

def log_message(message):
    """Log a message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def setup_airflow_env():
    """Set up Airflow environment variables"""
    os.environ['AIRFLOW_HOME'] = os.path.join(project_dir, 'airflow')
    log_message(f"Set AIRFLOW_HOME to: {os.environ['AIRFLOW_HOME']}")

def run_airflow_dag():
    """Run Airflow DAG manually"""
    dag_id = 'stock_data_pipeline'
    log_message(f"Running Airflow DAG: {dag_id}")
    
    try:
        # Check if DAG exists
        result = subprocess.run(
            ['airflow', 'dags', 'list'], 
            capture_output=True, 
            text=True
        )
        
        if dag_id not in result.stdout:
            log_message(f"DAG {dag_id} not found. Available DAGs:")
            log_message(result.stdout)
            return
        
        # Run DAG
        log_message("Triggering DAG run...")
        subprocess.run(['airflow', 'dags', 'trigger', dag_id])
        log_message(f"DAG {dag_id} triggered. Check Airflow UI or logs for results.")
        
    except Exception as e:
        log_message(f"Error running Airflow DAG: {str(e)}")

def main():
    """Main function"""
    log_message("Starting Airflow helper")
    
    # Set up Airflow environment
    setup_airflow_env()
    
    # Run Airflow DAG
    run_airflow_dag()
    
    log_message("Airflow helper completed")

if __name__ == "__main__":
    main()