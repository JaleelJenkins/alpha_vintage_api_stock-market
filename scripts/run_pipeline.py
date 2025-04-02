import subprocess
import time
import os
from datetime import datetime

def log_message(message):
    """Log a message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_dir = "../logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = f"{log_dir}/pipeline.log"
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    
    print(f"[{timestamp}] {message}")

def main():
    """Run the entire data pipeline"""
    log_message("Starting data pipeline")
    
    try:
        # Step 1: Extract data from API
        log_message("Starting data extraction")
        extract_result = subprocess.run(["python", "extract_stock_data.py"], 
                                        capture_output=True, text=True)
        
        if extract_result.returncode != 0:
            log_message(f"Error in extraction: {extract_result.stderr}")
            return
        
        log_message(f"Extraction completed: {extract_result.stdout.strip()}")
        
        # Step 2: Load data to SQLite
        log_message("Starting data loading to SQLite")
        load_result = subprocess.run(["python", "load_to_sqlite.py"], 
                                     capture_output=True, text=True)
        
        if load_result.returncode != 0:
            log_message(f"Error in loading: {load_result.stderr}")
            return
        
        log_message(f"Loading completed: {load_result.stdout.strip()}")
        
        # Step 3: Run transformations
        log_message("Running SQL transformations")
        transform_result = subprocess.run(["python", "execute_sql.py"], 
                                          capture_output=True, text=True)
        
        if transform_result.returncode != 0:
            log_message(f"Error in transformations: {transform_result.stderr}")
            return
        
        log_message(f"Transformations completed: {transform_result.stdout.strip()}")
        
        log_message("Pipeline completed successfully")
        
    except Exception as e:
        log_message(f"Pipeline failed with error: {str(e)}")

if __name__ == "__main__":
    main()