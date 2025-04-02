import sqlite3
import os

def execute_sql_file(db_path, sql_file):
    """Execute all SQL statements in a file"""
    # Read SQL file
    with open(sql_file, 'r') as f:
        sql_script = f.read()
    
    # Split into individual statements
    # This is a simple split - might need refinement for complex SQL
    statements = sql_script.split(';')
    
    # Connect to database
    conn = sqlite3.connect(db_path)
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
    
    print("SQL execution completed")

def main():
    db_path = "../data/stock_data.db"
    sql_file = "sqlite_transformations.sql"
    
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return
    
    if not os.path.exists(sql_file):
        print(f"SQL file not found: {sql_file}")
        return
    
    execute_sql_file(db_path, sql_file)

if __name__ == "__main__":
    main()