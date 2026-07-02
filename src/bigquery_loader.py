"""
Module: bigquery_loader
Responsibility: Load the processed Apache Parquet dataset into Google BigQuery.
"""
import os
import json
import time
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Constants
PARQUET_FILE_PATH = Path("data/processed/processed_tripdata.parquet")
CREDENTIALS_PATH = Path("config/gcp_credentials.json")
REPORT_PATH = Path("logs/bigquery_loader_report.json")

# ⚠️ UPDATE THESE VARIABLES WITH YOUR GCP DETAILS
PROJECT_ID = "urban-transit-sandbox-depi"
DATASET_ID = "transit_data"
TABLE_ID = "processed_tripdata"

def get_bq_client() -> bigquery.Client:
    """
    Authenticates and returns a Google BigQuery client using the Service Account JSON.
    """
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(f"Credentials file not found at: {CREDENTIALS_PATH}")
    
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH)
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_parquet_to_bigquery(client: bigquery.Client) -> dict:
    """
    Loads a Parquet file into BigQuery using WRITE_TRUNCATE mode for idempotency.
    Returns a report dictionary.
    """
    if not PARQUET_FILE_PATH.exists():
        raise FileNotFoundError(f"Parquet file not found at: {PARQUET_FILE_PATH}")

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Ensures idempotency
    )

    print(f"✓ Starting upload to BigQuery table: {table_ref}...")
    start_time = time.time()

    with open(PARQUET_FILE_PATH, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Wait for the job to complete
    execution_time = round(time.time() - start_time, 2)

    # Validation: Get the row count directly from BigQuery
    table = client.get_table(table_ref)
    
    report = {
        "status": "success",
        "bq_project_id": PROJECT_ID,
        "bq_table": table_ref,
        "rows_loaded": table.num_rows,
        "execution_time_seconds": execution_time
    }
    
    return report

def generate_report(report_data: dict) -> None:
    """Saves the BigQuery loading report as a JSON file."""
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=4)
    print(f"✓ BigQuery load report saved to: {REPORT_PATH}")

def main():
    try:
        print("Initializing BigQuery Loader...")
        client = get_bq_client()
        report = load_parquet_to_bigquery(client)
        generate_report(report)
        print(f"✓ Successfully loaded {report['rows_loaded']} rows into BigQuery in {report['execution_time_seconds']} seconds.")
        
    except Exception as e:
        print(f"❌ BigQuery Load failed: {e}")

if __name__ == "__main__":
    main()