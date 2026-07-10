from pathlib import Path
import polars as pl
from src.schema_validation import validate_schema
from src.cleaning import (
    clean_data,
    save_cleaning_report,
    save_cleaned_dataframe
)

PROJECT_DIR = Path("/opt/airflow/project")
INGESTED_DATA_PATH = PROJECT_DIR / "data" / "interim" / "ingested_tripdata.csv"

def run_cleaning_pipeline():
    print("Running Cleaning Pipeline...")
    
    # 1. Check if ingested data exists
    if not INGESTED_DATA_PATH.exists():
        raise FileNotFoundError(f"Ingested data missing: {INGESTED_DATA_PATH}")

    # 2. Read the ingested data directly (DO NOT rerun ingestion)
    print(f"Reading ingested data from: {INGESTED_DATA_PATH}")
    df = pl.read_csv(
        INGESTED_DATA_PATH, 
        infer_schema_length=10000,
        schema_overrides={"start_station_id": pl.Utf8, "end_station_id": pl.Utf8}
    )

    # 3. Validate Schema
    report = validate_schema(df)
    if not report["schema_valid"]:
        raise ValueError("Schema validation failed.")

    # 4. Clean Data
    cleaned_df, cleaning_report = clean_data(df)
    
    # 5. Save Outputs
    save_cleaning_report(cleaning_report)
    save_cleaned_dataframe(cleaned_df)

    print("\n✅ Data Cleaning Completed Successfully")
    print(f"Rows Before: {cleaning_report['initial_rows']}")
    print(f"Rows After: {cleaning_report['final_rows']}")
    print(f"Rows Removed: {cleaning_report['removed_rows']}")
    
    return cleaned_df

if __name__ == "__main__":
    run_cleaning_pipeline()