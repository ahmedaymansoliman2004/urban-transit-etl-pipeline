from __future__ import annotations

import time
import json
from pathlib import Path
from typing import Any, List, Dict

import polars as pl

# Define file paths based on the project architecture
INPUT_CSV_PATH = Path("data/interim/transformed_tripdata.csv")
OUTPUT_PARQUET_PATH = Path("data/processed/processed_tripdata.parquet")
REPORT_PATH = Path("logs/parquet_export_report.json")

# Define columns that must exist to consider the dataset valid
# (Adjust this list based on your actual transformation output)
REQUIRED_COLUMNS = [
    "ride_id", 
    "start_station_name", 
    "end_station_name", 
    "trip_duration_minutes"
]


def validate_input_file(file_path: Path) -> None:
    """
    Validates that the input CSV file exists and is not empty.
    
    Args:
        file_path (Path): The path to the input CSV file.
        
    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty (0 bytes).
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
        
    if file_path.stat().st_size == 0:
        raise ValueError(f"Input file is empty: {file_path}")


def load_dataset(file_path: Path, required_columns: List[str]) -> pl.DataFrame:
    """
    Loads the dataset using Polars and validates its schema.
    
    Args:
        file_path (Path): The path to the CSV file.
        required_columns (List[str]): A list of column names that must exist.
        
    Returns:
        pl.DataFrame: The loaded Polars DataFrame.
        
    Raises:
        ValueError: If the dataset has no rows or is missing required columns.
    """
    # Load dataset using Polars with schema overrides to prevent type inference errors
    df = pl.read_csv(
        file_path,
        schema_overrides={
            "start_station_id": pl.Utf8,
            "end_station_id": pl.Utf8
        }
    )
    
    # Validate row count
    if df.height == 0:
        raise ValueError("The loaded dataset contains 0 rows.")
        
    # Validate schema (check for required columns)
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Invalid schema. Missing required columns: {missing_columns}")
        
    return df


def export_parquet(df: pl.DataFrame, output_path: Path) -> float:
    """
    Exports the Polars DataFrame to a Parquet file using Snappy compression.
    
    Args:
        df (pl.DataFrame): The DataFrame to export.
        output_path (Path): The destination path for the Parquet file.
        
    Returns:
        float: The size of the generated Parquet file in Megabytes (MB).
        
    Raises:
        IOError: If writing to the destination path fails.
    """
    try:
        # Ensure the destination directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to Parquet with Snappy compression
        df.write_parquet(output_path, compression="snappy")
        
        # Calculate file size in MB
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        return round(file_size_mb, 2)
        
    except Exception as e:
        raise IOError(f"Failed to write Parquet file at {output_path}. Error: {e}")


def generate_report(
    status: str, 
    rows: int, 
    columns: int, 
    output_file: str, 
    file_size_mb: float, 
    execution_time: float
) -> Dict[str, Any]:
    """
    Generates and saves a JSON report containing export metadata.
    
    Args:
        status (str): The status of the export (e.g., "success", "failed").
        rows (int): Number of rows exported.
        columns (int): Number of columns exported.
        output_file (str): The path to the output file.
        file_size_mb (float): Size of the output file in MB.
        execution_time (float): The time taken to execute the export in seconds.
        
    Returns:
        Dict[str, Any]: The generated metadata dictionary.
    """
    report = {
        "status": status,
        "rows": rows,
        "columns": columns,
        "compression": "snappy",
        "output_file": output_file,
        "file_size_mb": file_size_mb,
        "execution_time_seconds": round(execution_time, 2)
    }
    
    # Ensure the logs directory exists
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Save the report to a JSON file
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)
        
    return report


def main() -> Dict[str, Any]:
    """
    Main orchestration function for the Parquet Export stage.
    
    Returns:
        Dict[str, Any]: The execution report metadata.
    """
    start_time = time.time()
    
    try:
        print("✓ Validating input file...")
        validate_input_file(INPUT_CSV_PATH)
        
        print("✓ Reading transformed dataset...")
        df = load_dataset(INPUT_CSV_PATH, REQUIRED_COLUMNS)
        
        print("✓ Exporting to Parquet with Snappy compression...")
        file_size_mb = export_parquet(df, OUTPUT_PARQUET_PATH)
        
        execution_time = time.time() - start_time
        
        print("✓ Generating export report...")
        report = generate_report(
            status="success",
            rows=df.height,
            columns=df.width,
            output_file=str(OUTPUT_PARQUET_PATH),
            file_size_mb=file_size_mb,
            execution_time=execution_time
        )
        
        print("\n✓ Export completed successfully!")
        print(f"  - Rows: {report['rows']:,}")
        print(f"  - Size: {report['file_size_mb']} MB")
        print(f"  - Time: {report['execution_time_seconds']} seconds\n")
        
        return report
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"\n❌ Export failed: {e}\n")
        
        # Generate a failure report
        generate_report(
            status="failed",
            rows=0,
            columns=0,
            output_file=str(OUTPUT_PARQUET_PATH),
            file_size_mb=0.0,
            execution_time=execution_time
        )
        raise


if __name__ == "__main__":
    main()