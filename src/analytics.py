import os
import sys
from google.cloud import bigquery

# Append base directory to sys.path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

def run_analytics():
    print("Starting Analytics Pipeline...")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=settings.GCP_PROJECT_ID)
    
    os.makedirs(settings.OUTPUT_DIR, exist_ok=True)
    
    # Define table reference dynamically
    table_ref = f"{settings.GCP_PROJECT_ID}.{settings.GCP_DATASET}.{settings.GCP_PROCESSED_TABLE}"
    
    top_stations_query = f"""
    SELECT 
        start_station_name, 
        COUNT(ride_id) AS total_trips
    FROM 
        `{table_ref}`
    WHERE 
        start_station_name IS NOT NULL 
        AND start_station_name != 'Unknown Station'
    GROUP BY 
        start_station_name
    ORDER BY 
        total_trips DESC
    LIMIT 10;
    """
    
    peak_hours_query = f"""
    SELECT 
        EXTRACT(HOUR FROM PARSE_TIME('%I:%M:%S %p', start_time)) AS hour_of_day, 
        COUNT(ride_id) AS total_trips
    FROM 
        `{table_ref}`
    WHERE 
        start_time IS NOT NULL
    GROUP BY 
        hour_of_day
    ORDER BY 
        total_trips DESC
    LIMIT 24;
    """
    
    # Execute queries and export results to CSV
    print("Executing Top Stations query...")
    top_stations_df = client.query(top_stations_query).to_dataframe()
    top_stations_path = os.path.join(settings.OUTPUT_DIR, "top_stations.csv")
    top_stations_df.to_csv(top_stations_path, index=False)
    print(f"Saved: {top_stations_path}")
    
    print("Executing Peak Hours query...")
    peak_hours_df = client.query(peak_hours_query).to_dataframe()
    peak_hours_path = os.path.join(settings.OUTPUT_DIR, "peak_hours.csv")
    peak_hours_df.to_csv(peak_hours_path, index=False)
    print(f"Saved: {peak_hours_path}")
    
    print("Analytics Pipeline completed successfully.")

if __name__ == "__main__":
    run_analytics()