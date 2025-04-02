#!/bin/bash

# Set Airflow environment variables
export AIRFLOW_HOME="$(pwd)/airflow"

# Print environment settings
echo "Airflow Home: $AIRFLOW_HOME"
echo "Project directory: $(pwd)"

# Setup instructions
echo "--------------------------------------"
echo "Airflow setup complete!"
echo "To start Airflow, run the following commands in separate terminals:"
echo ""
echo "Terminal 1 (Airflow webserver):"
echo "  export AIRFLOW_HOME=\"$(pwd)/airflow\""
echo "  airflow webserver --port 8080"
echo ""
echo "Terminal 2 (Airflow scheduler):"
echo "  export AIRFLOW_HOME=\"$(pwd)/airflow\""
echo "  airflow scheduler"
echo ""
echo "Then access the Airflow UI at: http://localhost:8080"
echo "  Username: admin"
echo "  Password: admin"
echo "--------------------------------------"
