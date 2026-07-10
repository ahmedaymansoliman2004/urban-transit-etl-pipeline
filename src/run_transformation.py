from pathlib import Path
import polars as pl
from src.transformation import (
    transform_data,
    save_transformation_report,
    save_transformed_dataframe,
)

from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent

INGESTED_DATA_PATH = (
    PROJECT_DIR
    / "data"
    / "interim"
    / "ingested_tripdata.csv"
)

CLEANED_DATA_PATH = (
    PROJECT_DIR
    / "data"
    / "interim"
    / "cleaned_tripdata.csv"
)

def run_transformation_pipeline():
    print("Running Transformation Pipeline...")

    # 1. Check Existence
    if not CLEANED_DATA_PATH.exists():
        raise FileNotFoundError(f"Cleaned data missing: {CLEANED_DATA_PATH}")
        
    if not INGESTED_DATA_PATH.exists():
        raise FileNotFoundError(f"Ingested data missing: {INGESTED_DATA_PATH}")

    # 2. Check Freshness (The Claude Fix)
    # Ensure cleaned data was generated AFTER the ingested data
    if CLEANED_DATA_PATH.stat().st_mtime < INGESTED_DATA_PATH.stat().st_mtime:
        raise RuntimeError(
            f"🚨 STALE DATA DETECTED: {CLEANED_DATA_PATH.name} is older than {INGESTED_DATA_PATH.name}. "
            "The cleaning task did not run on the current data."
        )

    # 3. Read Cleaned Data
    print(f"Reading cleaned data from: {CLEANED_DATA_PATH}")
    cleaned_df = pl.read_csv(
        CLEANED_DATA_PATH,
        infer_schema_length=10000,
        schema_overrides={"start_station_id": pl.Utf8, "end_station_id": pl.Utf8}
    )

    print("Starting Data Transformation...")

    # 4. Transform Data
    transformed_df, report = transform_data(cleaned_df)

    # 5. Save Outputs
    save_transformation_report(report)
    save_transformed_dataframe(transformed_df)

    print("✅ Data Transformation Completed!")
    print(f"Final Row Count: {transformed_df.height}")

    return transformed_df

if __name__ == "__main__":
    run_transformation_pipeline()
