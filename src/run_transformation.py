import polars as pl
from pathlib import Path
from transformation import (
    transform_data,
    save_transformation_report,
    save_transformed_dataframe
)

# Path to the cleaned data
CLEANED_DATA_PATH = Path("data/interim/cleaned_tripdata.csv")

def run_transformation_pipeline() -> pl.DataFrame:
    """Read cleaned data with explicit schema and apply transformations."""
    
    if not CLEANED_DATA_PATH.exists():
        raise FileNotFoundError(f"Cleaned data not found at {CLEANED_DATA_PATH}. Run cleaning first.")

    print(f"Reading cleaned data from: {CLEANED_DATA_PATH}")
    
    # Use schema_overrides to ensure station IDs are read as strings
    cleaned_df = pl.read_csv(
        CLEANED_DATA_PATH,
        schema_overrides={
            "start_station_id": pl.Utf8,
            "end_station_id": pl.Utf8
        }
    )

    # Start Transformation
    print("Starting Data Transformation...")
    transformed_df, transform_report = transform_data(cleaned_df)

    # Save outputs
    save_transformation_report(transform_report)
    save_transformed_dataframe(transformed_df)

    print("\n✅ Data Transformation Completed!")
    print(f"Final Row Count: {transform_report['final_rows']:,}")

    return transformed_df

if __name__ == "__main__":
    run_transformation_pipeline()